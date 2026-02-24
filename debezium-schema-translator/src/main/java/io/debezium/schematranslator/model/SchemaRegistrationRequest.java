package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Request body for POST /api/v1/schema-translator/register-schemas.
 */
public class SchemaRegistrationRequest {

    @JsonProperty("tables")
    private List<String> tables;

    public SchemaRegistrationRequest() {
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
