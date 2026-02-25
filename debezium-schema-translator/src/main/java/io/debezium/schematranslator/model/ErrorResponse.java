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

    public ErrorResponse(String message, List<String> errors) {
        this.error = new ErrorDetail(message, errors);
    }

    public ErrorDetail getError() {
        return error;
    }

    public static class ErrorDetail {

        @JsonProperty("message")
        private final String message;

        @JsonProperty("errors")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final List<String> errors;

        public ErrorDetail(String message, List<String> errors) {
            this.message = message;
            this.errors = errors;
        }

        public String getMessage() {
            return message;
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}