/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.debezium.config.Configuration;

/**
 * Configuration loaded from environment variables.
 * Provides independent config groups for PostgreSQL and Schema Registry.
 */
public class TranslatorConfig {

    // PostgreSQL connection settings
    private final String postgresHost;
    private final int postgresPort;
    private final String postgresDatabase;
    private final String postgresUser;
    private final String postgresPassword;
    private final String postgresSslMode;

    // Schema Registry settings
    private final String schemaRegistryUrl;

    // Service settings
    private final String topicPrefix;
    private final int httpPort;

    public TranslatorConfig() {
        this.postgresHost = getEnv("POSTGRES_HOST", "localhost");
        this.postgresPort = Integer.parseInt(getEnv("POSTGRES_PORT", "5432"));
        this.postgresDatabase = requireEnv("POSTGRES_DATABASE");
        this.postgresUser = getEnv("POSTGRES_USER", "postgres");
        this.postgresPassword = requireEnv("POSTGRES_PASSWORD");
        this.postgresSslMode = getEnv("POSTGRES_SSL_MODE", "prefer");

        this.schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        this.topicPrefix = requireEnv("TOPIC_PREFIX");
        this.httpPort = Integer.parseInt(getEnv("HTTP_PORT", "8080"));
    }

    /**
     * Builds the Debezium {@link Configuration} for the PostgreSQL connector side.
     */
    public Configuration toDebeziumConfig() {
        Properties props = new Properties();
        props.put("database.hostname", postgresHost);
        props.put("database.port", String.valueOf(postgresPort));
        props.put("database.dbname", postgresDatabase);
        props.put("database.user", postgresUser);
        props.put("database.password", postgresPassword);
        props.put("database.sslmode", postgresSslMode);
        props.put("topic.prefix", topicPrefix);
        // Required for Avro-compatible schema names
        props.put("schema.name.adjustment.mode", "avro");
        // Required by config validation, never used at runtime
        props.put("plugin.name", "pgoutput");
        props.put("slot.name", "dummy_slot");
        return Configuration.from(props);
    }

    /**
     * Builds the Schema Registry client configuration map.
     */
    public Map<String, String> toSchemaRegistryConfig() {
        Map<String, String> srProps = new HashMap<>();
        srProps.put("schema.registry.url", schemaRegistryUrl);
        return srProps;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable '" + name + "' is not set");
        }
        return value;
    }
}
