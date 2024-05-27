package fr.su.memorydb.handlers.insertion.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;

public class InsertResponse {

    @JsonProperty
    private String table;

    @JsonProperty
    private int rows;

    @JsonProperty
    Duration seconds;

    public InsertResponse(String table) {
        this.table = table;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public void setSeconds(Duration seconds) {
        this.seconds = seconds;
    }

}
