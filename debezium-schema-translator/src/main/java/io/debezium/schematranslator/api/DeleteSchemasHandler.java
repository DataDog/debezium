package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.debezium.schematranslator.model.ErrorResponse;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.model.SchemaDeletionResponse;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles DELETE /api/v1/schema-translator/schemas.
 */
public class DeleteSchemasHandler implements HttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSchemasHandler.class);

    private final ObjectMapper objectMapper;
    private final SchemaRegistryPublisher publisher;
    private final String topicPrefix;

    public DeleteSchemasHandler(SchemaRegistryPublisher publisher, String topicPrefix) {
        this.objectMapper = new ObjectMapper();
        this.publisher = publisher;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, new ErrorResponse("Method not allowed"));
            return;
        }

        List<String> subjects;
        try {
            subjects = publisher.getAllSubjects();
        }
        catch (IOException e) {
            LOGGER.error("Failed to retrieve subjects from Schema Registry", e);
            sendJson(exchange, 500, new ErrorResponse("Could not connect to the Schema Registry"));
            return;
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

    private void sendJson(HttpExchange exchange, int statusCode, Object body) throws IOException {
        byte[] responseBytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(responseBytes);
        }
    }
}
