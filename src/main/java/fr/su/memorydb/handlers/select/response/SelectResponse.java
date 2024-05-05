package fr.su.memorydb.handlers.select.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.controllers.TableSelection;

import java.util.*;
import java.util.stream.Collectors;


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

    public SelectResponse aggregate(TableSelection.SelectBody selectBody) {

        if(selectBody.hasGroupBy()) {

            HashMap<Integer, HashMap<String, Object>> groupBy = (HashMap<Integer, HashMap<String, Object>>) rows.entrySet().stream()
                    .filter(entry -> entry.getKey() < 0)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            for(int i : groupBy.keySet()) {

                for(String clm : groupBy.get(i).keySet()) {

                    int index = (int) groupBy.get(i).get(clm);
                    System.out.println("access " + i + " with index " + index);
                    rows.get(i).put(clm, rows.get(index).get(clm));
                }


            }


            //rows = groupBy;
            rows = (HashMap<Integer, HashMap<String, Object>>) rows.entrySet().stream()
                    .filter(entry -> entry.getKey() < 0)
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
            //plusieurs lignes
        }

        return null;
    }

    public void add(int index, HashMap<String, Object> row) {
        rows.put(index, row);
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public void remove(HashMap<String, Object> key) {
        rows.remove(key);
    }

    // Getter for rows to remove indexes
    public Collection<HashMap<String, Object>> getRows() {
        return rows.values();
    }

    public boolean containIndex(int index) {

        return rows.containsKey(index);
    }
}
