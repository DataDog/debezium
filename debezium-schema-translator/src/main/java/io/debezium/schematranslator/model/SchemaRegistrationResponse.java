package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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
