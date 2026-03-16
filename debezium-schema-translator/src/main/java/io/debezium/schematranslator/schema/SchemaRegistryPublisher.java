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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                String oldSchema;
                try {
                    oldSchema = client.getLatestSchemaMetadata(subject).getSchema();
                } catch (Exception fetchEx) {
                    LOGGER.warn("Could not fetch existing schema for subject '{}'", subject, fetchEx);
                    oldSchema = "could not retrieve existing schema: " + fetchEx.getMessage();
                }
                throw new SchemaIncompatibilityException(
                        "Schema for table " + tableName + " is incompatible with an earlier schema for subject \""
                                + subject + "\": " + e.getMessage(),
                        e,
                        oldSchema,
                        avroSchema.toString());
            }
            throw new IOException("Schema Registry error (HTTP " + e.getStatus() + "): " + e.getMessage(), e);
        }
    }

    /**
     * Returns all subject names currently registered in the Schema Registry.
     *
     * @return list of subject names
     * @throws IOException if a network or infrastructure error occurs
     */
    public List<String> getAllSubjects() throws IOException {
        try {
            return new ArrayList<>(client.getAllSubjects());
        }
        catch (RestClientException e) {
            throw new IOException("Schema Registry error (HTTP " + e.getStatus() + "): " + e.getMessage(), e);
        }
    }

    /**
     * Deletes all versions of the given subject from the Schema Registry.
     *
     * @param subject     the SR subject to delete
     * @param topicPrefix the topic prefix used to derive the table name
     * @return a {@link RegisteredSchema} describing the deleted subject
     * @throws IOException if a network or infrastructure error occurs
     */
    public RegisteredSchema deleteSubject(String subject, String topicPrefix) throws IOException {
        try {
            // Capture metadata before deletion — getLatestSchemaMetadata must precede deleteSubject
            // intentionally, as it would throw 404 afterwards.
            SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);
            int schemaId = metadata.getId();
            int version = metadata.getVersion();
            client.deleteSubject(subject);
            String table = deriveTableName(subject, topicPrefix);
            LOGGER.info("Deleted subject '{}' (table '{}', schema_id={}, version={})",
                    subject, table, schemaId, version);
            return new RegisteredSchema(table, subject, schemaId, version);
        }
        catch (RestClientException e) {
            throw new IOException("Schema Registry error (HTTP " + e.getStatus() + "): " + e.getMessage(), e);
        }
    }

    private static String deriveTableName(String subject, String topicPrefix) {
        String table = subject;
        if (topicPrefix != null && !topicPrefix.isEmpty()) {
            String prefix = topicPrefix + ".";
            if (table.startsWith(prefix)) {
                table = table.substring(prefix.length());
            }
        }
        if (table.endsWith("-value")) {
            table = table.substring(0, table.length() - "-value".length());
        } else if (table.endsWith("-key")) {
            table = table.substring(0, table.length() - "-key".length());
        }
        return table;
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
