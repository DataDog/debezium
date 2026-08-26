package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.schema.DebeziumSchemaReader;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeleteSchemasHandlerTest {

    @Mock
    private SchemaRegistryPublisher publisher;

    private DeleteSchemasHandler handler;
    private ByteArrayOutputStream responseBody;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        handler = new DeleteSchemasHandler(publisher, new DebeziumSchemaReader("test").getTopicNamer(), "test");
    }

    @Test
    void nonDeleteMethodReturns405() throws Exception {
        HttpExchange exchange = mockExchange("GET");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(405), anyLong());
    }

    @Test
    void getAllSubjectsFailureReturns500WithConnectMessage() throws Exception {
        HttpExchange exchange = mockExchange("DELETE");
        when(publisher.getAllSubjects()).thenThrow(new IOException("connection refused"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(500), anyLong());
        assertThat(errorMessage()).isEqualTo("Could not connect to the Schema Registry");
    }

    @Test
    void deleteSubjectFailureReturns500WithDeleteMessage() throws Exception {
        HttpExchange exchange = mockExchange("DELETE");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenThrow(new IOException("delete failed"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(500), anyLong());
        assertThat(errorMessage()).isEqualTo("Failed to delete schemas from schema registry");
    }

    @Test
    void successfulDeletionReturns200WithDeletedSchemas() throws Exception {
        HttpExchange exchange = mockExchange("DELETE");
        when(publisher.getAllSubjects())
                .thenReturn(List.of("test.public.users-value", "test.public.users-key"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-value", 1, 1));
        when(publisher.deleteSubject("test.public.users-key", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-key", 2, 1));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        JsonNode response = responseJson();
        assertThat(response.get("deleted_count").asInt()).isEqualTo(2);
        assertThat(response.get("deleted_schemas")).hasSize(2);
        JsonNode first = response.get("deleted_schemas").get(0);
        assertThat(first.get("subject").asText()).isEqualTo("test.public.users-value");
        assertThat(first.get("table").asText()).isEqualTo("public.users");
        assertThat(first.get("schema_id").asInt()).isEqualTo(1);
        assertThat(first.get("version").asInt()).isEqualTo(1);
        JsonNode second = response.get("deleted_schemas").get(1);
        assertThat(second.get("subject").asText()).isEqualTo("test.public.users-key");
        assertThat(second.get("table").asText()).isEqualTo("public.users");
        assertThat(second.get("schema_id").asInt()).isEqualTo(2);
        assertThat(second.get("version").asInt()).isEqualTo(1);
    }

    @Test
    void emptyRegistryReturns200WithEmptyArrayAndZeroCount() throws Exception {
        HttpExchange exchange = mockExchange("DELETE");
        when(publisher.getAllSubjects()).thenReturn(List.of());

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        JsonNode response = responseJson();
        assertThat(response.get("deleted_count").asInt()).isEqualTo(0);
        assertThat(response.get("deleted_schemas")).hasSize(0);
    }

    @Test
    void bodyWithoutTablesDeletesEverything() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"connection_string\":\"postgresql://x/y\"}");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-value", 1, 1));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        assertThat(responseJson().get("deleted_count").asInt()).isEqualTo(1);
    }

    @Test
    void requestedTablesDeleteOnlyTheirSubjects() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":[\"public.users\",\"events\"]}");
        when(publisher.getAllSubjects()).thenReturn(List.of(
                "test.public.users-value", "test.public.users-key",
                "test.public.events-value",
                "test.public.orders-value", "test.public.orders-key"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-value", 1, 1));
        when(publisher.deleteSubject("test.public.users-key", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-key", 2, 1));
        when(publisher.deleteSubject("test.public.events-value", "test"))
                .thenReturn(new RegisteredSchema("public.events", "test.public.events-value", 3, 1));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        JsonNode response = responseJson();
        assertThat(response.get("deleted_count").asInt()).isEqualTo(3);
        verify(publisher, never()).deleteSubject(eq("test.public.orders-value"), anyString());
        verify(publisher, never()).deleteSubject(eq("test.public.orders-key"), anyString());
    }

    @Test
    void duplicatedTableIsDeletedOnce() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":[\"users\",\"public.users\"]}");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenReturn(new RegisteredSchema("public.users", "test.public.users-value", 1, 1));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        assertThat(responseJson().get("deleted_count").asInt()).isEqualTo(1);
        verify(publisher, times(1)).deleteSubject("test.public.users-value", "test");
    }

    @Test
    void unknownTableReturns404AndDeletesNothing() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":[\"public.users\",\"public.missing\"]}");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(404), anyLong());
        assertThat(errorMessage()).isEqualTo("No registered schemas found for table(s): public.missing");
        verify(publisher, never()).deleteSubject(anyString(), anyString());
    }

    @Test
    void emptyTablesListReturns400() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":[]}");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
        assertThat(errorMessage()).isEqualTo("Field 'tables' must contain at least one entry when provided");
        verify(publisher, never()).getAllSubjects();
    }

    @Test
    void blankTableEntryReturns400() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":[\"  \"]}");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
        assertThat(errorMessage()).isEqualTo("Field 'tables' must not contain blank entries");
        verify(publisher, never()).deleteSubject(anyString(), anyString());
    }

    @Test
    void malformedBodyReturns400() throws Exception {
        HttpExchange exchange = mockExchange("DELETE", "{\"tables\":");

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(400), anyLong());
        assertThat(errorMessage()).startsWith("Invalid JSON request body:");
        verify(publisher, never()).getAllSubjects();
    }

    private JsonNode responseJson() throws IOException {
        return objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
    }

    private String errorMessage() throws IOException {
        return responseJson().get("error").get("message").asText();
    }

    private HttpExchange mockExchange(String method) throws IOException {
        return mockExchange(method, "");
    }

    private HttpExchange mockExchange(String method, String requestBody) throws IOException {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestMethod()).thenReturn(method);
        when(exchange.getResponseHeaders()).thenReturn(new Headers());
        lenient().when(exchange.getRequestBody())
                .thenReturn(new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)));
        responseBody = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(responseBody);
        return exchange;
    }
}
