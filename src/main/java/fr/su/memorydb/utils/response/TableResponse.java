package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Column;

import java.util.LinkedList;
import java.util.List;

public class TableResponse extends Response {

    @JsonProperty
    private List<String> columns;

    public TableResponse(String tableName) {
        super(tableName);
        columns = new LinkedList<>();
    }

    public void addColumn(String column) {
        columns.add(column);
    }

    public List<String> getColumns() {
        return columns;
    }

}
