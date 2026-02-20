/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for a successful POST /api/v1/schema-translator/register-schemas.
 */
public class SchemaRegistrationResponse {

    @JsonProperty("registered_schemas")
    private List<RegisteredSchema> registeredSchemas;

    public SchemaRegistrationResponse() {
    }

    public SchemaRegistrationResponse(List<RegisteredSchema> registeredSchemas) {
        this.registeredSchemas = registeredSchemas;
    }

    public List<RegisteredSchema> getRegisteredSchemas() {
        return registeredSchemas;
    }

    public void setRegisteredSchemas(List<RegisteredSchema> registeredSchemas) {
        this.registeredSchemas = registeredSchemas;
    }
}
