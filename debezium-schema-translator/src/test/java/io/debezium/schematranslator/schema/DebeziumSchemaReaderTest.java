package io.debezium.schematranslator.schema;

import io.debezium.config.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumSchemaReaderTest {

    @Test
    void buildConfigParsesFullUrl() {
        Configuration config = DebeziumSchemaReader.buildConfig(
                "postgresql://alice:secret@pg.example.com:6543/llm_obs?sslmode=require",
                "topicPrefix");

        assertThat(config.getString("database.hostname")).isEqualTo("pg.example.com");
        assertThat(config.getString("database.port")).isEqualTo("6543");
        assertThat(config.getString("database.dbname")).isEqualTo("llm_obs");
        assertThat(config.getString("database.user")).isEqualTo("alice");
        assertThat(config.getString("database.password")).isEqualTo("secret");
        assertThat(config.getString("database.sslmode")).isEqualTo("require");
        assertThat(config.getString("topic.prefix")).isEqualTo("topicPrefix");
    }

    @Test
    void buildConfigDefaultsPortAndSslMode() {
        Configuration config = DebeziumSchemaReader.buildConfig(
                "postgresql://alice:secret@pg.example.com/llm_obs",
                "topicPrefix");

        assertThat(config.getString("database.port")).isEqualTo("5432");
        assertThat(config.getString("database.sslmode")).isEqualTo("prefer");
    }

    @Test
    void buildConfigAcceptsPostgresScheme() {
        Configuration config = DebeziumSchemaReader.buildConfig(
                "postgres://alice:secret@pg.example.com/llm_obs",
                "topicPrefix");

        assertThat(config.getString("database.hostname")).isEqualTo("pg.example.com");
    }

    @Test
    void buildConfigDecodesPercentEncodedCredentials() {
        Configuration config = DebeziumSchemaReader.buildConfig(
                "postgresql://us%40r:p%40ss@pg.example.com/llm_obs",
                "topicPrefix");

        assertThat(config.getString("database.user")).isEqualTo("us@r");
        assertThat(config.getString("database.password")).isEqualTo("p@ss");
    }

    @Test
    void buildConfigHandlesUsernameOnly() {
        Configuration config = DebeziumSchemaReader.buildConfig(
                "postgresql://alice@pg.example.com/llm_obs",
                "topicPrefix");

        assertThat(config.getString("database.user")).isEqualTo("alice");
        assertThat(config.getString("database.password")).isEqualTo("");
    }

    @Test
    void buildConfigRejectsNullOrBlank() {
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(null, "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig("   ", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void buildConfigRejectsWrongScheme() {
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(
                "mysql://alice:secret@pg.example.com/llm_obs", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("postgresql://");
    }

    @Test
    void buildConfigRejectsMissingHost() {
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(
                "postgresql:///llm_obs", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the host");
    }

    @Test
    void buildConfigRejectsMissingDatabase() {
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(
                "postgresql://alice:secret@pg.example.com", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the database");
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(
                "postgresql://alice:secret@pg.example.com/", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing the database");
    }

    @Test
    void buildConfigRejectsMalformedUrl() {
        assertThatThrownBy(() -> DebeziumSchemaReader.buildConfig(
                "not a valid uri", "topicPrefix"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid connection_string");
    }
}
