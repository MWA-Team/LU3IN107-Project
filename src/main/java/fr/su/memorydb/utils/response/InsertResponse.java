package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InsertResponse extends ValidResponse {

    @JsonProperty
    private int rows;

    public InsertResponse(String table) {
        super(table);
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

}
