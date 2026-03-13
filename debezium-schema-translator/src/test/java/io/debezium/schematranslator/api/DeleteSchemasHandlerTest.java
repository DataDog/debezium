package io.debezium.schematranslator.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.debezium.schematranslator.model.RegisteredSchema;
import io.debezium.schematranslator.schema.SchemaRegistryPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
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
        handler = new DeleteSchemasHandler(publisher, "test");
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
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText())
                .isEqualTo("Could not connect to the Schema Registry");
    }

    @Test
    void deleteSubjectFailureReturns500WithDeleteMessage() throws Exception {
        HttpExchange exchange = mockExchange("DELETE");
        when(publisher.getAllSubjects()).thenReturn(List.of("test.public.users-value"));
        when(publisher.deleteSubject("test.public.users-value", "test"))
                .thenThrow(new IOException("delete failed"));

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(500), anyLong());
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("error").get("message").asText())
                .isEqualTo("Failed to delete schemas from schema registry");
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
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
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
        JsonNode response = objectMapper.readTree(responseBody.toString(StandardCharsets.UTF_8));
        assertThat(response.get("deleted_count").asInt()).isEqualTo(0);
        assertThat(response.get("deleted_schemas")).hasSize(0);
    }

    private HttpExchange mockExchange(String method) throws IOException {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestMethod()).thenReturn(method);
        when(exchange.getResponseHeaders()).thenReturn(new Headers());
        responseBody = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(responseBody);
        return exchange;
    }
}
