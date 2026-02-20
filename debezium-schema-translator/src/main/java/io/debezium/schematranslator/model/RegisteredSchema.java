/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-table registration result.
 */
public class RegisteredSchema {

    @JsonProperty("table")
    private String table;

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("schema_id")
    private int schemaId;

    @JsonProperty("version")
    private int version;

    public RegisteredSchema() {
    }

    public RegisteredSchema(String table, String subject, int schemaId, int version) {
        this.table = table;
        this.subject = subject;
        this.schemaId = schemaId;
        this.version = version;
    }

    public String getTable() {
        return table;
    }

    public String getSubject() {
        return subject;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getVersion() {
        return version;
    }
}
