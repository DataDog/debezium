package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Error response body following Google API style:
 * {@code {"error": {"message": "...", "errors": [...]}}}
 *
 * <p>{@code errors} is omitted when there is only a single error (e.g. 400, 405, 500).
 * It is populated for 409 schema-incompatibility responses so all failing tables
 * are reported in one round-trip.
 */
public class ErrorResponse {

    @JsonProperty("error")
    private final ErrorDetail error;

    public ErrorResponse(String message) {
        this.error = new ErrorDetail(message, null);
    }

    public ErrorResponse(String message, List<SchemaError> errors) {
        this.error = new ErrorDetail(message, errors);
    }

    public ErrorDetail getError() {
        return error;
    }

    public static class SchemaError {

        @JsonProperty("message")
        private final String message;

        @JsonProperty("old_schema")
        private final String oldSchema;

        @JsonProperty("new_schema")
        private final String newSchema;

        public SchemaError(String message, String oldSchema, String newSchema) {
            this.message = message;
            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
        }

        public String getMessage() {
            return message;
        }

        public String getOldSchema() {
            return oldSchema;
        }

        public String getNewSchema() {
            return newSchema;
        }
    }

    public static class ErrorDetail {

        @JsonProperty("message")
        private final String message;

        @JsonProperty("errors")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final List<SchemaError> errors;

        public ErrorDetail(String message, List<SchemaError> errors) {
            this.message = message;
            this.errors = errors;
        }

        public String getMessage() {
            return message;
        }

        public List<SchemaError> getErrors() {
            return errors;
        }
    }
}