package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class DebugRowsResponse extends ValidResponse {

    @JsonProperty
    HashMap<String, Integer> rows;

    public DebugRowsResponse() {}

    public DebugRowsResponse(HashMap<String, Integer> rows) {
        super();
        this.rows = rows;
    }

    public HashMap<String, Integer> getRows() {
        return rows;
    }

}
