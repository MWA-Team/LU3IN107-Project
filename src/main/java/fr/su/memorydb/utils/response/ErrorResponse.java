package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorResponse extends Response {

    @JsonProperty
    private String error;

    public ErrorResponse(String table, String error) {
        super(table);
        this.error = error;
    }
}
