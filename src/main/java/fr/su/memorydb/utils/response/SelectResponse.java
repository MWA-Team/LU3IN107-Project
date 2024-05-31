package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.controllers.TableSelection;
import jakarta.ws.rs.core.Link;

import java.util.*;

public class SelectResponse extends ValidResponse {

    @JsonProperty
    private List<HashMap<String, Object>> rows;

    public SelectResponse() {}

    public SelectResponse(String tableName, List<HashMap<String, Object>> rows) {
        super(tableName);
        this.rows = rows;
    }

    public List<HashMap<String, Object>> getRows() {
        return rows;
    }

}
