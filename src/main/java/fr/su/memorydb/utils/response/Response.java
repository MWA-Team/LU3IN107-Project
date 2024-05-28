package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;

public abstract class Response {

    @JsonProperty
    private String table;

    public Response(String table) {
        this.table = table;
    }
}
