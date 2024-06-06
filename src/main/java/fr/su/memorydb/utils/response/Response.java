package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;

public abstract class Response {

    @JsonProperty
    @JsonInclude(JsonInclude. Include. NON_NULL)
    private String table;

    @JsonIgnore
    private Instant start;

    @JsonProperty
    private Duration duration;

    public Response() {}

    public Response(String table) {
        this.start = Instant.now();
        this.table = table;
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

}
