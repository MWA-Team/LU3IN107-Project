package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DetailsResponse extends ResponseTime {

    @JsonProperty
    private String details;

    public DetailsResponse(String table, String details) {
        super(table);
        this.details = details;
    }

    public DetailsResponse(String table) {
        this(table, null);
    }

    public DetailsResponse details(String details) {
        this.details = details;
        return this;
    }
}
