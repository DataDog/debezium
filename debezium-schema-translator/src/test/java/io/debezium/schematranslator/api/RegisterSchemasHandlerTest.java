package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.schema.AvroSchemaConverter;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher.SchemaIncompatibilityException;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RegisterSchemasHandlerTest {

    @Mock
    private DebeziumSchemaReader schemaReader;
    @Mock
    private AvroSchemaConverter avroConverter;
    @Mock
    private SchemaRegistryPublisher publisher;

    private RegisterSchemasHandler handler;
    private ByteArrayOutputStream responseBody;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        handler = new RegisterSchemasHandler(schemaReader, avroConverter, publisher);
    }

    @Test
    void nonPostMethodReturns405() throws Exception {
        HttpExchange exchange = mockExchange("GET", "");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(405), anyLong());
    }

    @Test
    void invalidJsonBodyReturns400() throws Exception {
        HttpExchange exchange = mockExchange("POST", "not-json");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
    }

    @Test
    void missingTablesFieldReturns400() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{}");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText()).contains("tables");
    }

    @Test
    void emptyTablesListReturns400() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[]}");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
    }

    @Test
    void postgresErrorReturns500() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[\"public.users\"]}");
        when(schemaReader.readSchemas(anyList())).thenThrow(new RuntimeException("Connection refused"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(500), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText()).contains("Connection refused");
    }

    @Test
    void schemaIncompatibilityReturns409() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[\"public.users\"]}");
        setupSchemaReaderAndConverter();
        doThrow(new SchemaIncompatibilityException("incompatible schema", new Exception(),
                "{\"old\":true}", "{\"new\":true}"))
                .when(publisher).register(any(), any(), any());

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(409), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText())
                .isEqualTo("One or more schemas are incompatible with an existing version");
        assertThat(response.get("error").get("errors")).hasSize(1);
        JsonNode error = response.get("error").get("errors").get(0);
        assertThat(error.get("message").asText()).contains("incompatible schema");
        assertThat(error.get("old_schema").asText()).isEqualTo("{\"old\":true}");
        assertThat(error.get("new_schema").asText()).isEqualTo("{\"new\":true}");
    }

    @Test
    void multipleSchemaIncompatibilitiesReturnsAllErrors() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[\"public.users\",\"public.orders\"]}");
        setupSchemaReaderAndConverterForTables();
        doThrow(new SchemaIncompatibilityException("incompatible schema for users", new Exception(),
                "{\"old\":\"users\"}", "{\"new\":\"users\"}"))
                .when(publisher).register(eq("public.users"), any(), any());
        doThrow(new SchemaIncompatibilityException("incompatible schema for orders", new Exception(),
                "{\"old\":\"orders\"}", "{\"new\":\"orders\"}"))
                .when(publisher).register(eq("public.orders"), any(), any());

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(409), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        JsonNode errors = response.get("error").get("errors");
        assertThat(errors).hasSize(2);
        assertThat(errors.get(0).get("message").asText()).contains("incompatible schema for users");
        assertThat(errors.get(0).get("old_schema").asText()).isEqualTo("{\"old\":\"users\"}");
        assertThat(errors.get(0).get("new_schema").asText()).isEqualTo("{\"new\":\"users\"}");
        assertThat(errors.get(1).get("message").asText()).contains("incompatible schema for orders");
        assertThat(errors.get(1).get("old_schema").asText()).isEqualTo("{\"old\":\"orders\"}");
        assertThat(errors.get(1).get("new_schema").asText()).isEqualTo("{\"new\":\"orders\"}");
    }

    @Test
    void schemaRegistryIoErrorReturns500() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[\"public.users\"]}");
        setupSchemaReaderAndConverter();
        doThrow(new IOException("network error"))
                .when(publisher).register(any(), any(), any());

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(500), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText()).contains("network error");
    }

    @Test
    void successfulRegistrationReturns200WithResults() throws Exception {
        HttpExchange exchange = mockExchange("POST", "{\"tables\":[\"public.users\"]}");
        setupSchemaReaderAndConverter();
        when(publisher.register(eq("public.users"), eq("test.public.users-value"), any()))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-value", 1, 1));
        when(publisher.register(eq("public.users"), eq("test.public.users-key"), any()))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-key", 2, 1));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("registered_schemas")).hasSize(2);
        JsonNode value = response.get("registered_schemas").get(0);
        assertThat(value.get("subject").asText()).isEqualTo("test.public.users-value");
        assertThat(value.get("schema_id").asInt()).isEqualTo(1);
        JsonNode key = response.get("registered_schemas").get(1);
        assertThat(key.get("subject").asText()).isEqualTo("test.public.users-key");
        assertThat(key.get("schema_id").asInt()).isEqualTo(2);
    }

    @SuppressWarnings("unchecked")
    private void setupSchemaReaderAndConverterForTables() throws Exception {
        TableId usersId = new TableId(null, "public", "users");
        TableId ordersId = new TableId(null, "public", "orders");

        TableSchema usersSchema = mock(TableSchema.class);
        Envelope usersEnvelope = mock(Envelope.class);
        when(usersSchema.getEnvelopeSchema()).thenReturn(usersEnvelope);
        when(usersEnvelope.schema()).thenReturn(SchemaBuilder.struct().name("UsersEnvelope").build());
        when(usersSchema.keySchema()).thenReturn(null);

        TableSchema ordersSchema = mock(TableSchema.class);
        Envelope ordersEnvelope = mock(Envelope.class);
        when(ordersSchema.getEnvelopeSchema()).thenReturn(ordersEnvelope);
        when(ordersEnvelope.schema()).thenReturn(SchemaBuilder.struct().name("OrdersEnvelope").build());
        when(ordersSchema.keySchema()).thenReturn(null);

        Map<TableId, TableSchema> schemas = new LinkedHashMap<>();
        schemas.put(usersId, usersSchema);
        schemas.put(ordersId, ordersSchema);
        when(schemaReader.readSchemas(List.of("public.users", "public.orders"))).thenReturn(schemas);

        TopicNamingStrategy<TableId> strategy = mock(TopicNamingStrategy.class);
        when(strategy.dataChangeTopic(usersId)).thenReturn("test.public.users");
        when(strategy.dataChangeTopic(ordersId)).thenReturn("test.public.orders");
        when(schemaReader.getTopicNamingStrategy()).thenReturn(strategy);

        when(avroConverter.toAvro(any()))
                .thenReturn(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    }

    @SuppressWarnings("unchecked")
    private void setupSchemaReaderAndConverter() throws Exception {
        TableId tableId = new TableId(null, "public", "users");
        TableSchema tableSchema = mock(TableSchema.class);
        Envelope envelope = mock(Envelope.class);
        when(tableSchema.getEnvelopeSchema()).thenReturn(envelope);
        when(envelope.schema()).thenReturn(SchemaBuilder.struct().name("Envelope").build());
        when(tableSchema.keySchema()).thenReturn(SchemaBuilder.struct().name("Key")
                .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA).build());

        Map<TableId, TableSchema> schemas = new LinkedHashMap<>();
        schemas.put(tableId, tableSchema);
        when(schemaReader.readSchemas(List.of("public.users"))).thenReturn(schemas);

        TopicNamingStrategy<TableId> strategy = mock(TopicNamingStrategy.class);
        when(strategy.dataChangeTopic(tableId)).thenReturn("test.public.users");
        when(schemaReader.getTopicNamingStrategy()).thenReturn(strategy);

        when(avroConverter.toAvro(any()))
                .thenReturn(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    }

    private HttpExchange mockExchange(String method, String body) throws IOException {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestMethod()).thenReturn(method);
        lenient().when(exchange.getRequestBody())
                .thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        when(exchange.getResponseHeaders()).thenReturn(new Headers());
        responseBody = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(responseBody);
        return exchange;
    }
}
