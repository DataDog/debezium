/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schematranslator.model.ErrorResponse;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.model.SchemaRegistrationRequest;
import io.debezium.schematranslator.model.SchemaRegistrationResponse;
import io.debezium.schematranslator.schema.AvroSchemaConverter;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher.SchemaIncompatibilityException;

/**
 * Handles POST /api/v1/schema-translator/register-schemas.
 */
public class RegisterSchemasHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterSchemasHandler.class);

    private final ObjectMapper objectMapper;
    private final DebeziumSchemaReader schemaReader;
    private final AvroSchemaConverter avroConverter;
    private final SchemaRegistryPublisher publisher;

    public RegisterSchemasHandler(DebeziumSchemaReader schemaReader,
                                  AvroSchemaConverter avroConverter,
                                  SchemaRegistryPublisher publisher) {
        this.objectMapper = new ObjectMapper();
        this.schemaReader = schemaReader;
        this.avroConverter = avroConverter;
        this.publisher = publisher;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, new ErrorResponse("Method not allowed"));
            return;
        }

        // Parse request body
        SchemaRegistrationRequest request;
        try (InputStream body = exchange.getRequestBody()) {
            request = objectMapper.readValue(body, SchemaRegistrationRequest.class);
        }
        catch (Exception e) {
            sendJson(exchange, 400, new ErrorResponse("Invalid JSON request body: " + e.getMessage()));
            return;
        }

        // Validate tables field
        if (request.getTables() == null || request.getTables().isEmpty()) {
            sendJson(exchange, 400,
                    new ErrorResponse("Field 'tables' is required and must contain at least one entry"));
            return;
        }

        List<String> tables = request.getTables();
        LOGGER.info("Processing register-schemas request for {} table(s): {}", tables.size(), tables);

        // Read schemas from PostgreSQL
        Map<TableId, TableSchema> tableSchemas;
        try {
            tableSchemas = schemaReader.readSchemas(tables);
        }
        catch (Exception e) {
            LOGGER.error("Failed to read schemas from PostgreSQL", e);
            sendJson(exchange, 500, new ErrorResponse(e.getMessage()));
            return;
        }

        // Register each schema with Schema Registry
        List<RegisteredSchema> results = new ArrayList<>();
        int tableIndex = 0;
        for (Map.Entry<TableId, TableSchema> entry : tableSchemas.entrySet()) {
            TableId tableId = entry.getKey();
            TableSchema tableSchema = entry.getValue();
            String originalTableName = tables.get(tableIndex++);

            // Derive subject from topic name
            String topic = schemaReader.getTopicNamingStrategy().dataChangeTopic(tableId);
            String subject = topic + "-value";

            // Convert envelope schema from Connect to Avro
            org.apache.kafka.connect.data.Schema envelopeConnectSchema = tableSchema.getEnvelopeSchema().schema();
            org.apache.avro.Schema avroSchema = avroConverter.toAvro(envelopeConnectSchema);

            // Register in Schema Registry
            try {
                RegisteredSchema registered = publisher.register(originalTableName, subject, avroSchema);
                results.add(registered);
            }
            catch (SchemaIncompatibilityException e) {
                LOGGER.warn("Schema incompatibility for table '{}': {}", originalTableName, e.getMessage());
                sendJson(exchange, 409, new ErrorResponse(e.getMessage()));
                return;
            }
            catch (IOException e) {
                LOGGER.error("Failed to register schema for table '{}'", originalTableName, e);
                sendJson(exchange, 500, new ErrorResponse(
                        "Failed to register schema for table " + originalTableName + ": " + e.getMessage()));
                return;
            }
        }

        sendJson(exchange, 200, new SchemaRegistrationResponse(results));
    }

    private void sendJson(HttpExchange exchange, int statusCode, Object body) throws IOException {
        byte[] responseBytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(responseBytes);
        }
    }
}
