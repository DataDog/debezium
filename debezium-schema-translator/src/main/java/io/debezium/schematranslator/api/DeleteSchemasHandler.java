package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.debezium.schematranslator.model.ErrorResponse;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.model.SchemaDeletionRequest;
import io.debezium.schematranslator.model.SchemaDeletionResponse;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import io.debezium.schematranslator.schema.TopicNamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Handles DELETE /api/v1/schema-translator/schemas.
 *
 * <p>The request body is optional. When it is absent, or carries no {@code tables} field, every
 * subject in the Schema Registry is deleted. When {@code tables} is set, only the {@code -value}
 * and {@code -key} subjects of those tables are deleted; both are assumed to be registered.
 */
public class DeleteSchemasHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSchemasHandler.class);

    private final ObjectMapper objectMapper;
    private final SchemaRegistryPublisher publisher;
    private final TopicNamer topicNamer;
    private final String topicPrefix;

    public DeleteSchemasHandler(SchemaRegistryPublisher publisher, TopicNamer topicNamer, String topicPrefix) {
        this.objectMapper = new ObjectMapper();
        this.publisher = publisher;
        this.topicNamer = topicNamer;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, new ErrorResponse("Method not allowed"));
            return;
        }

        SchemaDeletionRequest request;
        try {
            request = readRequest(exchange);
        }
        catch (Exception e) {
            sendJson(exchange, 400, new ErrorResponse("Invalid JSON request body: " + e.getMessage()));
            return;
        }

        List<String> tables = request == null ? null : request.getTables();
        if (tables != null && tables.isEmpty()) {
            sendJson(exchange, 400,
                    new ErrorResponse("Field 'tables' must contain at least one entry when provided"));
            return;
        }

        Collection<String> subjects;
        if (tables == null) {
            try {
                subjects = publisher.getAllSubjects();
            }
            catch (IOException e) {
                LOGGER.error("Failed to retrieve subjects from Schema Registry", e);
                sendJson(exchange, 500, new ErrorResponse("Could not connect to the Schema Registry"));
                return;
            }
            LOGGER.info("Deleting all {} subject(s) from Schema Registry", subjects.size());
        }
        else {
            LOGGER.info("Deleting schemas for {} table(s): {}", tables.size(), tables);
            // A set so a table listed twice, or under both its qualified and unqualified name,
            // is deleted once
            Set<String> requested = new LinkedHashSet<>();
            for (String table : tables) {
                try {
                    requested.add(topicNamer.valueSubject(table));
                    requested.add(topicNamer.keySubject(table));
                }
                catch (IllegalArgumentException e) {
                    sendJson(exchange, 400, new ErrorResponse(e.getMessage()));
                    return;
                }
            }
            subjects = requested;
        }

        List<RegisteredSchema> deletedSchemas = new ArrayList<>();
        for (String subject : subjects) {
            try {
                deletedSchemas.add(publisher.deleteSubject(subject, topicPrefix));
            }
            catch (IOException e) {
                LOGGER.error("Failed to delete subject '{}'", subject, e);
                sendJson(exchange, 500, new ErrorResponse("Failed to delete schemas from schema registry"));
                return;
            }
        }

        LOGGER.info("Deleted {} subject(s) from Schema Registry", deletedSchemas.size());
        sendJson(exchange, 200, new SchemaDeletionResponse(deletedSchemas));
    }

    /**
     * Reads the optional request body, returning {@code null} when no body was sent.
     */
    private SchemaDeletionRequest readRequest(HttpExchange exchange) throws IOException {
        byte[] body;
        try (InputStream in = exchange.getRequestBody()) {
            body = in == null ? new byte[0] : in.readAllBytes();
        }
        if (new String(body, StandardCharsets.UTF_8).isBlank()) {
            return null;
        }
        return objectMapper.readValue(body, SchemaDeletionRequest.class);
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
