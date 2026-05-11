package io.debezium.schematranslator.config;

/**
 * Service-level configuration loaded from environment variables.
 * Postgres connection details are not part of this config — they are supplied per-request
 * by the caller via the {@code connection_string} field in the request body.
 */
public class TranslatorConfig {

    private final String schemaRegistryUrl;
    private final String topicPrefix;
    private final int httpPort;

    public TranslatorConfig() {
        this.schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        this.topicPrefix = requireEnv("TOPIC_PREFIX");
        this.httpPort = Integer.parseInt(getEnv("HTTP_PORT", "8080"));
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
