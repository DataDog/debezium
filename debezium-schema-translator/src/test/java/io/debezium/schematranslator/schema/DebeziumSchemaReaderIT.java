package io.debezium.schematranslator.schema;

import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class DebeziumSchemaReaderIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17")
            .withCommand("postgres", "-c", "wal_level=logical",
                    "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4");

    private DebeziumSchemaReader reader;

    @BeforeEach
    void setUp() {
        reader = new DebeziumSchemaReader("test");
    }

    @Test
    void readsSchemasForTableWithCorrectFields() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS public.users (" +
                "  id SERIAL PRIMARY KEY," +
                "  name VARCHAR(100) NOT NULL," +
                "  email VARCHAR(255)" +
                ")");

        Map<TableId, TableSchema> schemas = reader.readSchemas(connectionString(), java.util.List.of("public.users"));

        assertThat(schemas).hasSize(1);
        TableSchema tableSchema = schemas.values().iterator().next();
        assertThat(tableSchema.valueSchema().fields())
                .extracting(org.apache.kafka.connect.data.Field::name)
                .containsExactlyInAnyOrder("id", "name", "email");
    }

    @Test
    void throwsForUnknownTable() {
        assertThatThrownBy(() -> reader.readSchemas(connectionString(), java.util.List.of("public.nonexistent")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Table not found: public.nonexistent");
    }

    @Test
    void unqualifiedTableNameDefaultsToPublicSchema() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS public.orders (" +
                "  id SERIAL PRIMARY KEY," +
                "  amount NUMERIC NOT NULL" +
                ")");

        Map<TableId, TableSchema> schemas = reader.readSchemas(connectionString(), java.util.List.of("orders"));

        assertThat(schemas).hasSize(1);
        TableId tableId = schemas.keySet().iterator().next();
        assertThat(tableId.schema()).isEqualTo("public");
        assertThat(tableId.table()).isEqualTo("orders");
    }

    private static String connectionString() {
        return String.format("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
                postgres.getUsername(), postgres.getPassword(),
                postgres.getHost(), postgres.getMappedPort(5432),
                postgres.getDatabaseName());
    }

    private void execute(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
