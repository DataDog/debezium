package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response body for DELETE /api/v1/schema-translator/schemas.
 */
public class SchemaDeletionResponse {

    @JsonProperty("deleted_schemas")
    private List<RegisteredSchema> deletedSchemas;

    @JsonProperty("deleted_count")
    private int deletedCount;

    public SchemaDeletionResponse(List<RegisteredSchema> deletedSchemas) {
        this.deletedSchemas = deletedSchemas;
        this.deletedCount = deletedSchemas.size();
    }

    public List<RegisteredSchema> getDeletedSchemas() {
        return deletedSchemas;
    }

    public int getDeletedCount() {
        return deletedCount;
    }
}
