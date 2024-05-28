package fr.su.memorydb.handlers.insertion.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.utils.response.Response;
import fr.su.memorydb.utils.response.ResponseTime;

import java.time.Duration;

public class InsertResponse extends ResponseTime {

    @JsonProperty
    private int rows;

    public InsertResponse(String table) {
        super(table);
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

}
