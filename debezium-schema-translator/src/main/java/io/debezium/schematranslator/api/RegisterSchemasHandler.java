package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schematranslator.model.ErrorResponse;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.model.SchemaRegistrationRequest;
import io.debezium.schematranslator.model.SchemaRegistrationResponse;
import io.debezium.schematranslator.model.ColumnEvolution;
import io.debezium.schematranslator.schema.AvroSchemaConverter;
import io.debezium.schematranslator.schema.AvroSchemaDiffAnalyzer;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher.SchemaIncompatibilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles POST /api/v1/schema-translator/schemas.
 */
public class RegisterSchemasHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterSchemasHandler.class);

    private final ObjectMapper objectMapper;
    private final DebeziumSchemaReader schemaReader;
    private final AvroSchemaConverter avroConverter;
    private final SchemaRegistryPublisher publisher;
    private final AvroSchemaDiffAnalyzer diffAnalyzer;

    public RegisterSchemasHandler(DebeziumSchemaReader schemaReader,
                                  AvroSchemaConverter avroConverter,
                                  SchemaRegistryPublisher publisher) {
        this.objectMapper = new ObjectMapper();
        this.schemaReader = schemaReader;
        this.avroConverter = avroConverter;
        this.publisher = publisher;
        this.diffAnalyzer = new AvroSchemaDiffAnalyzer();
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

        // Validate connection_string field
        String connectionString = request.getConnectionString();
        if (connectionString == null || connectionString.isBlank()) {
            sendJson(exchange, 400,
                    new ErrorResponse("Field 'connection_string' is required"));
            return;
        }

        List<String> tables = request.getTables();
        LOGGER.info("Processing schemas request for {} table(s): {}", tables.size(), tables);

        // Read schemas from Postgres
        Map<TableId, TableSchema> tableSchemas;
        try {
            tableSchemas = schemaReader.readSchemas(connectionString, tables);
        }
        catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid request: {}", e.getMessage());
            sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
            return;
        }
        catch (Exception e) {
            LOGGER.error("Failed to read schemas from Postgres", e);
            sendJson(exchange, 500, new ErrorResponse(e.getMessage()));
            return;
        }

        // Register each schema with Schema Registry
        List<RegisteredSchema> results = new ArrayList<>();
        List<ErrorResponse.SchemaError> incompatibilityErrors = new ArrayList<>();
        int tableIndex = 0;
        for (Map.Entry<TableId, TableSchema> entry : tableSchemas.entrySet()) {
            TableId tableId = entry.getKey();
            TableSchema tableSchema = entry.getValue();
            String originalTableName = tables.get(tableIndex++);

            // Derive subjects from topic name
            String topic = schemaReader.getTopicNamingStrategy().dataChangeTopic(tableId);
            String valueSubject = topic + "-value";
            String keySubject = topic + "-key";

            // Convert schemas from Connect to Avro
            org.apache.kafka.connect.data.Schema envelopeConnectSchema = tableSchema.getEnvelopeSchema().schema();
            org.apache.avro.Schema valueAvroSchema = avroConverter.toAvro(envelopeConnectSchema);

            org.apache.kafka.connect.data.Schema keyConnectSchema = tableSchema.keySchema();

            // Register value schema (envelope) and, when a primary key exists, key schema
            try {
                results.add(publisher.register(originalTableName, valueSubject, valueAvroSchema));
                if (keyConnectSchema != null) {
                    org.apache.avro.Schema keyAvroSchema = avroConverter.toAvro(keyConnectSchema);
                    results.add(publisher.register(originalTableName, keySubject, keyAvroSchema));
                }
            }
            catch (SchemaIncompatibilityException e) {
                LOGGER.warn("Schema incompatibility for table '{}': {}", originalTableName, e.getMessage());
                List<ColumnEvolution> columns = diffAnalyzer.diff(e.getOldSchema(), e.getNewSchema());
                incompatibilityErrors.add(new ErrorResponse.SchemaError(
                        e.getMessage(), e.getOldSchema(), e.getNewSchema(),
                        columns.isEmpty() ? null : columns));
            }
            catch (IOException e) {
                LOGGER.error("Failed to register schema for table '{}'", originalTableName, e);
                sendJson(exchange, 500, new ErrorResponse(
                        "Failed to register schema for table " + originalTableName + ": " + e.getMessage()));
                return;
            }
        }

        if (!incompatibilityErrors.isEmpty()) {
            sendJson(exchange, 409, new ErrorResponse("One or more schemas are incompatible with an existing version",
                    incompatibilityErrors));
            return;
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
