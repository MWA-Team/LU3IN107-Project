package fr.su.memorydb.handlers.select.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.controllers.TableSelection;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;


public class SelectResponse {

    @JsonProperty
    private HashMap<Integer, HashMap<String, Object>> rows = new HashMap<>();

    public SelectResponse() {
        this.rows = new HashMap<>();
    }

    public SelectResponse merge(List<SelectResponse> responses) {
        if(responses == null || responses.isEmpty()) { return this; }

        SelectResponse retval = new SelectResponse();

        if (rows.isEmpty())
            return retval;

        //If group by is in the responses, we need to add it before updating
        for(SelectResponse selectResponse : responses) {
            for(int index : selectResponse.rows.keySet()) {
                if(index < 0) {
                    HashMap<String, Object> columns = selectResponse.rows.get(index);
                    int groupByIndex = (int)columns.entrySet().stream().findFirst().get().getValue();

                    retval.add(index, columns);
                }
            }
        }

        for (Map.Entry<Integer, HashMap<String, Object>> row : rows.entrySet()) {
            boolean valid = true;
            int index = (Integer) row.getKey();
            HashMap<String, Object> newRow = new HashMap<>();
            for (SelectResponse response : responses) {
                if (response == null)
                    continue;
                HashMap<String, Object> tmp = response.rows.get(index);
                if (row.getKey() > 0 && tmp == null) {
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

        try {
            if(InetAddress.getLocalHost().getHostAddress().equals("192.168.1.20")) {

                return this;
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        if(selectBody.hasGroupBy()) {

            HashMap<Integer, HashMap<String, Object>> groupBy = (HashMap<Integer, HashMap<String, Object>>) rows.entrySet().stream()
                    .filter(entry -> entry.getKey() < 0)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            for(int i : groupBy.keySet()) {

                for(String clm : groupBy.get(i).keySet()) {

                    if(groupBy.get(i).get(clm) instanceof Integer index) {

                        rows.get(i).put(clm, rows.get(index).get(clm));
                    }
                }


            }

        }

        return this;
    }

    public void add(int index, HashMap<String, Object> row) {
        rows.put(index, row);
    }

    public void remove(HashMap<String, Object> key) {
        rows.remove(key);
    }

    public boolean containIndex(int index) {

        return rows.containsKey(index);
    }
}
