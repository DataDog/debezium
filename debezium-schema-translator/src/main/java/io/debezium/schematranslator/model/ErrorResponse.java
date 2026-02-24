package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error response body.
 */
public class ErrorResponse {

    @JsonProperty("error")
    private String error;

    public ErrorResponse() {
    }

    public ErrorResponse(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
