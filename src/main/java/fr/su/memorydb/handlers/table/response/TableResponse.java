package fr.su.memorydb.handlers.table.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.utils.response.DetailsResponse;

import java.util.LinkedList;
import java.util.List;

public class TableResponse extends DetailsResponse {

    @JsonProperty
    private List<Column> columns;

    public TableResponse(String tableName) {
        super(tableName);
        columns = new LinkedList<>();
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public List<Column> getColumns() {
        return columns;
    }

}
