package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class ValidResponse extends Response {

    @JsonProperty
    String details;

    public ValidResponse(String table) {
        super(table);
        this.details = null;
    }

    public ValidResponse(String table, String details) {
        super(table);
        this.details = details;
    }

    public ValidResponse() {}

    public ValidResponse details(String details) {
        this.details = details;
        return this;
    }

}
