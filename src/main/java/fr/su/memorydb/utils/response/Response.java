package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;

public abstract class Response {

    @JsonProperty
    private String table;

    @JsonProperty
    private String details;

    @JsonIgnore
    protected Instant start;

    @JsonProperty
    private Duration duration;

    public Response(String table) {
        this.table = table;
        this.start = Instant.now();
        this.details = null;
    }

    public Response(String table, String details) {
        this.table = table;
        this.start = Instant.now();
        this.details = details;
    }

    public Response done() {
        this.duration = Duration.between(start, Instant.now());
        return this;
    }

    public Instant getStart() {
        return start;
    }

    public Response setStart(Instant start) {
        this.start = start;
        return this;
    }

    public String getTable() {
        return table;
    }

    public Response details(String details) {
        this.details = details;
        return this;
    }

}
