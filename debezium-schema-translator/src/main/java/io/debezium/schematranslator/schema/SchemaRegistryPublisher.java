package io.debezium.schematranslator.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.debezium.schematranslator.model.RegisteredSchema;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * Registers Avro schemas with the Confluent Schema Registry.
 */
public class SchemaRegistryPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryPublisher.class);

    private final SchemaRegistryClient client;

    public SchemaRegistryPublisher(String schemaRegistryUrl) {
        this.client = new CachedSchemaRegistryClient(
                Collections.singletonList(schemaRegistryUrl), 1000);
    }

    /**
     * Registers the given Avro schema under the given subject.
     *
     * @param tableName the original table name string (for reporting)
     * @param subject   the SR subject (e.g. "prefix.public.users-value")
     * @param avroSchema the Avro schema to register
     * @return the registered schema result with schema_id and version
     * @throws SchemaIncompatibilityException if the schema is incompatible with an existing one (HTTP 409)
     * @throws IOException                    if a network or infrastructure error occurs
     */
    public RegisteredSchema register(String tableName, String subject, Schema avroSchema)
            throws SchemaIncompatibilityException, IOException {
        LOGGER.info("Registering schema for subject '{}' (table '{}')", subject, tableName);
        try {
            int schemaId = client.register(subject, new AvroSchema(avroSchema));
            SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);
            int version = metadata.getVersion();
            LOGGER.info("Registered subject '{}': schema_id={}, version={}", subject, schemaId, version);
            return new RegisteredSchema(tableName, subject, schemaId, version);
        }
        catch (RestClientException e) {
            if (e.getStatus() == 409) {
                String oldSchema = null;
                String incompatibilityMessage = "Schema for table " + tableName
                        + " is incompatible with an earlier schema for subject \""
                        + subject + "\": " + e.getMessage();
                try {
                    oldSchema = client.getLatestSchemaMetadata(subject).getSchema();
                } catch (Exception fetchEx) {
                    LOGGER.warn("Could not fetch existing schema for subject '{}'", subject, fetchEx);
                    incompatibilityMessage += " (could not retrieve existing schema: " + fetchEx.getMessage() + ")";
                }
                throw new SchemaIncompatibilityException(incompatibilityMessage, e, oldSchema, avroSchema.toString());
            }
            throw new IOException("Schema Registry error (HTTP " + e.getStatus() + "): " + e.getMessage(), e);
        }
    }

    /**
     * Thrown when a 409 Conflict is returned by the Schema Registry.
     */
    public static class SchemaIncompatibilityException extends Exception {
        private final String oldSchema;
        private final String newSchema;

        public SchemaIncompatibilityException(String message, Throwable cause, String oldSchema, String newSchema) {
            super(message, cause);
            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
        }

        public String getOldSchema() {
            return oldSchema;
        }

        public String getNewSchema() {
            return newSchema;
        }
    }
}
