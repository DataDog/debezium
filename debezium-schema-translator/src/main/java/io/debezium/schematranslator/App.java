package io.debezium.schematranslator;

import com.sun.net.httpserver.HttpServer;
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
        server.createContext(
                "/api/v1/schema-translator/schemas",
                new RegisterSchemasHandler(schemaReader, avroConverter, publisher));
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
