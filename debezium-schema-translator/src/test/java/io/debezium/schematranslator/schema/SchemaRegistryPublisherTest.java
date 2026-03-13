package io.debezium.schematranslator.schema;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher.SchemaIncompatibilityException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class SchemaRegistryPublisherTest {

    private static final Schema AVRO_SCHEMA = Schema.create(Schema.Type.STRING);

    @Test
    void registerReturnsSchemaIdAndVersion() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.register(eq("test.public.users-value"), any(ParsedSchema.class))).thenReturn(42);
            when(client.getLatestSchemaMetadata("test.public.users-value"))
                    .thenReturn(new SchemaMetadata(42, 3, "{}"));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            RegisteredSchema result = publisher.register("public.users", "test.public.users-value", AVRO_SCHEMA);

            assertThat(result.getTable()).isEqualTo("public.users");
            assertThat(result.getSubject()).isEqualTo("test.public.users-value");
            assertThat(result.getSchemaId()).isEqualTo(42);
            assertThat(result.getVersion()).isEqualTo(3);
        }
    }

    @Test
    void register409ThrowsSchemaIncompatibilityException() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.register(any(), any(ParsedSchema.class)))
                    .thenThrow(new RestClientException("Schema being registered is incompatible", 409, 409));
            when(client.getLatestSchemaMetadata(any()))
                    .thenReturn(new SchemaMetadata(1, 1, "\"string\""));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            assertThatThrownBy(() -> publisher.register("public.users", "test.public.users-value", AVRO_SCHEMA))
                    .isInstanceOf(SchemaIncompatibilityException.class)
                    .hasMessageContaining("public.users")
                    .hasMessageContaining("test.public.users-value")
                    .satisfies(ex -> {
                        SchemaIncompatibilityException sie = (SchemaIncompatibilityException) ex;
                        assertThat(sie.getOldSchema()).isEqualTo("\"string\"");
                        assertThat(sie.getNewSchema()).isEqualTo(AVRO_SCHEMA.toString());
                    });
        }
    }

    @Test
    void register409WithFetchFailureIncludesReasonInMessage() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.register(any(), any(ParsedSchema.class)))
                    .thenThrow(new RestClientException("Schema being registered is incompatible", 409, 409));
            when(client.getLatestSchemaMetadata(any()))
                    .thenThrow(new IOException("connection refused"));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            assertThatThrownBy(() -> publisher.register("public.users", "test.public.users-value", AVRO_SCHEMA))
                    .isInstanceOf(SchemaIncompatibilityException.class)
                    .satisfies(ex -> {
                        SchemaIncompatibilityException sie = (SchemaIncompatibilityException) ex;
                        assertThat(sie.getOldSchema()).isEqualTo("could not retrieve existing schema: connection refused");
                        assertThat(sie.getNewSchema()).isEqualTo(AVRO_SCHEMA.toString());
                    });
        }
    }

    @Test
    void registerNon409RestClientExceptionThrowsIOException() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.register(any(), any(ParsedSchema.class)))
                    .thenThrow(new RestClientException("Internal Server Error", 500, 50001));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            assertThatThrownBy(() -> publisher.register("public.users", "test.public.users-value", AVRO_SCHEMA))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("HTTP 500");
        }
    }

    @Test
    void getAllSubjectsReturnsSubjectList() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.getAllSubjects()).thenReturn(List.of("test.public.users-value", "test.public.users-key"));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            List<String> subjects = publisher.getAllSubjects();

            assertThat(subjects).containsExactlyInAnyOrder("test.public.users-value", "test.public.users-key");
        }
    }

    @Test
    void getAllSubjectsWrapsRestClientExceptionIntoIOException() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.getAllSubjects()).thenThrow(new RestClientException("Unauthorized", 401, 40101));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            assertThatThrownBy(() -> publisher.getAllSubjects())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("HTTP 401");
        }
    }

    @Test
    void deleteSubjectReturnsCorrectRegisteredSchema() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.getLatestSchemaMetadata("test.public.users-value"))
                    .thenReturn(new SchemaMetadata(42, 3, "{}"));
            when(client.deleteSubject("test.public.users-value")).thenReturn(List.of(1, 2, 3));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            RegisteredSchema result = publisher.deleteSubject("test.public.users-value", "test");

            assertThat(result.getTable()).isEqualTo("public.users");
            assertThat(result.getSubject()).isEqualTo("test.public.users-value");
            assertThat(result.getSchemaId()).isEqualTo(42);
            assertThat(result.getVersion()).isEqualTo(3);
        }
    }

    @Test
    void deleteSubjectWrapsRestClientExceptionIntoIOException() throws Exception {
        try (var mock = mockConstruction(CachedSchemaRegistryClient.class, (client, ctx) -> {
            when(client.getLatestSchemaMetadata(any()))
                    .thenThrow(new RestClientException("Subject not found", 404, 40401));
        })) {
            SchemaRegistryPublisher publisher = new SchemaRegistryPublisher("http://localhost:8081");

            assertThatThrownBy(() -> publisher.deleteSubject("test.public.users-value", "test"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("HTTP 404");
        }
    }
}
