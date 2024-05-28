package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;

public abstract class ResponseTime extends Response {

    @JsonIgnore
    protected Instant start;

    @JsonProperty
    private Duration duration;

    public ResponseTime(String table) {
        super(table);
        this.start = Instant.now();
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

}
