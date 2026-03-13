package io.debezium.schematranslator;

import com.sun.net.httpserver.HttpServer;
import io.debezium.schematranslator.api.DeleteSchemasHandler;
import io.debezium.schematranslator.api.HealthHandler;
import io.debezium.schematranslator.api.RegisterSchemasHandler;
import io.debezium.schematranslator.config.TranslatorConfig;
import io.debezium.schematranslator.schema.AvroSchemaConverter;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Main entry point for the Debezium Schema Translator service.
 */
public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        TranslatorConfig config = new TranslatorConfig();

        LOGGER.info("Starting Debezium Schema Translator on port {}", config.getHttpPort());

        // Initialise core components
        DebeziumSchemaReader schemaReader = new DebeziumSchemaReader(config.toDebeziumConfig());
        AvroSchemaConverter avroConverter = new AvroSchemaConverter();
        SchemaRegistryPublisher publisher = new SchemaRegistryPublisher(config.getSchemaRegistryUrl());

        // Start HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(config.getHttpPort()), 0);
        RegisterSchemasHandler registerHandler = new RegisterSchemasHandler(schemaReader, avroConverter, publisher);
        DeleteSchemasHandler deleteHandler = new DeleteSchemasHandler(publisher, config.getTopicPrefix());
        server.createContext("/api/v1/schema-translator/schemas", exchange -> {
            String method = exchange.getRequestMethod();
            if ("POST".equalsIgnoreCase(method)) {
                registerHandler.handle(exchange);
            } else if ("DELETE".equalsIgnoreCase(method)) {
                deleteHandler.handle(exchange);
            } else {
                byte[] body = "{\"error\":{\"message\":\"Method not allowed\"}}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(405, body.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
        });
        server.createContext(
                "/api/v1/schema-translator/health",
                new HealthHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        LOGGER.info("Debezium Schema Translator is running on port {}", config.getHttpPort());

        // Register shutdown hook to stop the HTTP server and close the JDBC connection cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down...");
            server.stop(0);
            try {
                schemaReader.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error during shutdown", e);
            }
        }));
    }
}
