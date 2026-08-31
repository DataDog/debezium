package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Request body for DELETE /api/v1/schema-translator/schemas.
 *
 * <p>The body itself is optional: when absent, or when {@code tables} is not set, every subject in
 * the Schema Registry is deleted. Unknown properties are ignored so clients can reuse the
 * {@link SchemaRegistrationRequest} shape against the same path.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaDeletionRequest {

    @JsonProperty("tables")
    private List<String> tables;

    public SchemaDeletionRequest() {
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
