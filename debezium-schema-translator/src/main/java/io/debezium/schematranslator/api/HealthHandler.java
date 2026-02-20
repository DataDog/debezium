/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Handles GET /api/v1/schema-translator/health.
 * Returns {"status": "UP"} with HTTP 200.
 */
public class HealthHandler implements HttpHandler {

    private static final byte[] BODY = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, BODY.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(BODY);
        }
    }
}
