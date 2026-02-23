/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.sun.net.httpserver.HttpServer;

import io.debezium.config.Configuration;
import io.debezium.schematranslator.schema.AvroSchemaConverter;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;

/**
 * End-to-end integration test for {@link RegisterSchemasHandler}.
 * <p>
 * Exercises the full registration pipeline:
 * HTTP request → handler → PostgreSQL (Testcontainers) → Avro conversion
 * → real Confluent Schema Registry (Testcontainers) → HTTP response.
 * <p>
 * All three containers share a Docker network so Schema Registry can reach
 * Kafka's internal broker listener. A fresh component chain is created per
 * test to prevent Confluent client cache carryover between tests.
 */
@Testcontainers
class RegisterSchemasHandlerIT {

    // -------------------------------------------------------------------------
    // Shared Docker network and containers (started once for the test class)
    // -------------------------------------------------------------------------

    private static final Network network = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17")
            .withCommand("postgres", "-c", "wal_level=logical",
                    "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4");

    @Container
    @SuppressWarnings("deprecation")
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .dependsOn(kafka)
            .waitingFor(Wait.forHttp("/subjects")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    // -------------------------------------------------------------------------
    // Per-test handler server (recreated to avoid Confluent client cache carryover)
    // -------------------------------------------------------------------------

    private HttpServer handlerServer;
    private int handlerPort;
    private DebeziumSchemaReader reader;

    @BeforeEach
    void setUp() throws Exception {
        reader = new DebeziumSchemaReader(buildConfig());
        String srUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081);
        SchemaRegistryPublisher publisher = new SchemaRegistryPublisher(srUrl);
        RegisterSchemasHandler handler = new RegisterSchemasHandler(reader, new AvroSchemaConverter(), publisher);

        handlerServer = HttpServer.create(new InetSocketAddress(0), 0);
        handlerServer.createContext("/api/v1/schema-translator/register-schemas", handler);
        handlerServer.start();
        handlerPort = handlerServer.getAddress().getPort();
    }

    @AfterEach
    void tearDown() throws Exception {
        handlerServer.stop(0);
        reader.close();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void registersSchemaEndToEnd() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS public.products (" +
                "  id SERIAL PRIMARY KEY," +
                "  name VARCHAR(100) NOT NULL," +
                "  price NUMERIC" +
                ")");

        HttpURLConnection conn = post("{\"tables\":[\"public.products\"]}");
        String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(conn.getResponseCode()).isEqualTo(200);
        assertThat(body).contains("\"subject\":\"test.public.products-value\"");
        assertThat(body).contains("\"subject\":\"test.public.products-key\"");
        assertThat(body.split("\"table\":\"public.products\"", -1)).hasSize(3); // 2 occurrences → 3 parts
    }

    /**
     * Verifies that adding a nullable column to an existing table produces a new
     * schema version (v2) on re-registration.
     * <p>
     * Adding a nullable column is backward-compatible in Avro because Debezium
     * maps it to an optional field (null union + default), so the real Schema
     * Registry accepts it as v2.
     */
    @Test
    void evolvedSchemaIsRegisteredAsVersion2() throws Exception {
        execute("DROP TABLE IF EXISTS public.evolution_ok");
        execute("CREATE TABLE public.evolution_ok (" +
                "  id SERIAL PRIMARY KEY," +
                "  name TEXT NOT NULL" +
                ")");

        // First registration: value and key schemas at version 1
        HttpURLConnection conn1 = post("{\"tables\":[\"public.evolution_ok\"]}");
        assertThat(conn1.getResponseCode()).isEqualTo(200);
        conn1.getInputStream().readAllBytes(); // drain

        // Evolve the table: nullable column → backward-compatible Avro optional field
        execute("ALTER TABLE public.evolution_ok ADD COLUMN notes TEXT");

        // Second registration: value schema has a new optional field → version 2
        HttpURLConnection conn2 = post("{\"tables\":[\"public.evolution_ok\"]}");
        String body = new String(conn2.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(conn2.getResponseCode()).isEqualTo(200);
        assertThat(body).contains("\"subject\":\"test.public.evolution_ok-value\"");
        assertThat(body).contains("\"version\":2");
    }

    /**
     * Verifies that a backward-incompatible schema change is rejected by the real
     * Schema Registry with a 409, and that the handler surfaces it as a 409 response.
     * <p>
     * Adding a NOT NULL column without a default produces a required (non-nullable)
     * Avro field. Because the real Schema Registry enforces BACKWARD compatibility
     * by default, it rejects this change without any mocking.
     */
    @Test
    void incompatibleEvolutionReturns409() throws Exception {
        execute("DROP TABLE IF EXISTS public.evolution_bad");
        execute("CREATE TABLE public.evolution_bad (" +
                "  id SERIAL PRIMARY KEY," +
                "  name TEXT NOT NULL" +
                ")");

        // First registration: v1 succeeds
        HttpURLConnection conn1 = post("{\"tables\":[\"public.evolution_bad\"]}");
        assertThat(conn1.getResponseCode()).isEqualTo(200);
        conn1.getInputStream().readAllBytes(); // drain

        // Evolve the table: NOT NULL column without a default → required Avro field
        // → backward-incompatible; the empty table allows the DDL without a DEFAULT.
        execute("ALTER TABLE public.evolution_bad ADD COLUMN required_flag TEXT NOT NULL");

        // The real Schema Registry rejects the incompatible schema with 409
        HttpURLConnection conn2 = post("{\"tables\":[\"public.evolution_bad\"]}");
        int responseCode = conn2.getResponseCode(); // triggers the response; populates getErrorStream()
        java.io.InputStream errStream = conn2.getErrorStream();
        String errorBody = errStream != null ? new String(errStream.readAllBytes(), StandardCharsets.UTF_8) : "";
        assertThat(responseCode).isEqualTo(409);
        assertThat(errorBody).contains("Schema for table public.evolution_bad is incompatible with an earlier schema for subject");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Configuration buildConfig() {
        Properties props = new Properties();
        props.put("database.hostname", postgres.getHost());
        props.put("database.port", String.valueOf(postgres.getMappedPort(5432)));
        props.put("database.dbname", postgres.getDatabaseName());
        props.put("database.user", postgres.getUsername());
        props.put("database.password", postgres.getPassword());
        props.put("database.sslmode", "disable");
        props.put("topic.prefix", "test");
        props.put("schema.name.adjustment.mode", "avro");
        props.put("plugin.name", "pgoutput");
        props.put("slot.name", "dummy_slot");
        return Configuration.from(props);
    }

    private HttpURLConnection post(String jsonBody) throws Exception {
        URL url = new URL("http://localhost:" + handlerPort
                + "/api/v1/schema-translator/register-schemas");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        conn.getOutputStream().write(bytes);
        conn.getOutputStream().flush();
        return conn;
    }

    private void execute(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
