package io.debezium.schematranslator.schema;

import io.debezium.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumSchemaReaderTest {

    @Test
    void parseJdbcConfigParsesFullUrl() {
        JdbcConfiguration jdbc = DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://alice:secret@pg.example.com:6543/llm_obs?sslmode=require");

        assertThat(jdbc.getHostname()).isEqualTo("pg.example.com");
        assertThat(jdbc.getPort()).isEqualTo(6543);
        assertThat(jdbc.getDatabase()).isEqualTo("llm_obs");
        assertThat(jdbc.getUser()).isEqualTo("alice");
        assertThat(jdbc.getPassword()).isEqualTo("secret");
        assertThat(jdbc.getString("sslmode")).isEqualTo("require");
    }

    @Test
    void parseJdbcConfigDefaultsPortAndSslMode() {
        JdbcConfiguration jdbc = DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://alice:secret@pg.example.com/llm_obs");

        assertThat(jdbc.getPort()).isEqualTo(5432);
        assertThat(jdbc.getString("sslmode")).isEqualTo("prefer");
    }

    @Test
    void parseJdbcConfigAcceptsPostgresScheme() {
        JdbcConfiguration jdbc = DebeziumSchemaReader.parseJdbcConfig(
                "postgres://alice:secret@pg.example.com/llm_obs");

        assertThat(jdbc.getHostname()).isEqualTo("pg.example.com");
    }

    @Test
    void parseJdbcConfigDecodesPercentEncodedCredentials() {
        JdbcConfiguration jdbc = DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://us%40r:p%40ss@pg.example.com/llm_obs");

        assertThat(jdbc.getUser()).isEqualTo("us@r");
        assertThat(jdbc.getPassword()).isEqualTo("p@ss");
    }

    @Test
    void parseJdbcConfigHandlesUsernameOnly() {
        JdbcConfiguration jdbc = DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://alice@pg.example.com/llm_obs");

        assertThat(jdbc.getUser()).isEqualTo("alice");
        assertThat(jdbc.getPassword()).isEqualTo("");
    }

    @Test
    void parseJdbcConfigRejectsNullOrBlank() {
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void parseJdbcConfigRejectsWrongScheme() {
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(
                "mysql://alice:secret@pg.example.com/llm_obs"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("postgresql://");
    }

    @Test
    void parseJdbcConfigRejectsMissingHost() {
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(
                "postgresql:///llm_obs"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the host");
    }

    @Test
    void parseJdbcConfigRejectsMissingDatabase() {
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://alice:secret@pg.example.com"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the database");
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(
                "postgresql://alice:secret@pg.example.com/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the database");
    }

    @Test
    void parseJdbcConfigRejectsMalformedUrl() {
        assertThatThrownBy(() -> DebeziumSchemaReader.parseJdbcConfig(
                "not a valid uri"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid connection_string");
    }
}
