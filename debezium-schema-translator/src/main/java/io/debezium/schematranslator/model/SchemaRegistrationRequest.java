/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

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
