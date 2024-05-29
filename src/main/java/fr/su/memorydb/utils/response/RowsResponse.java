package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;

public class RowsResponse {

    @JsonProperty
    private String table;

    @JsonProperty
    private List<HashMap<String, Object>> rows;

    public RowsResponse() {}

    public RowsResponse(String table, List<HashMap<String, Object>> rows) {
        this.table = table;
        this.rows = rows;
    }

    public String getTable() {
        return table;
    }

    public List<HashMap<String, Object>> getRows() {
        return rows;
    }

}
