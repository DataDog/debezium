package io.debezium.schematranslator.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Handles GET /api/v1/schema-translator/health.
 * Returns {"status": "UP"} with HTTP 200.
 */
public class HealthHandler implements HttpHandler {

    private static final byte[] BODY = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, BODY.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(BODY);
        }
    }
}
