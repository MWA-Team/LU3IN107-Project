package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

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

    public static List<HashMap<String, Object>> mergeRows(List<List<HashMap<String, Object>>> rows) {
        if (rows == null || rows.isEmpty())
            return null;

        // Verifying that all the lists have the same length
        int length = -1;
        for (List<HashMap<String, Object>> row : rows) {
            if (row == null)
                continue;
            if (length == -1)
                length = row.size();
            else if (row.size() != length)
                throw new RuntimeException("You're trying to merge rows with different length !");
        }

        // Now length has the length of all lists and if it is -1, no rows were given
        if (length == -1)
            return null;

        // Actually merging the rows
        List<HashMap<String, Object>> mergedRows = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            for (List<HashMap<String, Object>> row : rows) {
                if (row == null)
                    continue;
                HashMap<String, Object> mergedRow;
                if (mergedRows.size() <= i) {
                    mergedRow = row.get(i);
                    mergedRows.add(mergedRow);
                } else {
                    mergedRow = mergedRows.get(i);
                    mergedRow.putAll(row.get(i));
                }
            }
        }

        return mergedRows;
    }

}
