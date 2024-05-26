package fr.su.memorydb.handlers.select.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;


public class SelectResponse {

    @JsonProperty
    private HashMap<Integer, HashMap<String, Object>> rows = new HashMap<>();

    public SelectResponse merge(List<SelectResponse> responses) {
        if(responses == null || responses.isEmpty()) { return this; }

        SelectResponse retval = new SelectResponse();

        if (rows.isEmpty())
            return retval;

        for (Map.Entry<Integer, HashMap<String, Object>> row : rows.entrySet()) {
            boolean valid = true;
            HashMap<String, Object> newRow = new HashMap<>();
            for (SelectResponse response : responses) {
                if (response == null)
                    continue;
                HashMap<String, Object> tmp = response.rows.get((Integer) row.getKey());
                if (tmp == null) {
                    valid = false;
                    break;
                }
                newRow.putAll(tmp);
            }
            if (valid) {
                newRow.putAll(row.getValue());
                retval.add(row.getKey(), newRow);
            }
        }
        return retval;
    }

    public void add(int index, HashMap<String, Object> row) {
        rows.put(index, row);
    }

}
