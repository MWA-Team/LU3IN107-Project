package fr.su.memorydb.handlers.select.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.utils.response.Response;
import fr.su.memorydb.utils.response.ResponseTime;

import java.time.Instant;
import java.util.*;

public class SelectResponse extends ResponseTime {

    @JsonProperty
    private HashMap<Integer, HashMap<String, Object>> rows = new HashMap<>();

    public SelectResponse(String tableName) {
        super(tableName);
        this.rows = new HashMap<>();
    }

    public SelectResponse(String tableName, Instant instant) {
        super(tableName);
        this.rows = new HashMap<>();
        this.start = instant;
    }

    public SelectResponse merge(List<SelectResponse> responses, TableSelection.SelectBody selectBody) {
        if(responses == null || responses.isEmpty()) { return this; }

        SelectResponse retval = new SelectResponse(selectBody.getTable(), this.start);

        if (rows.isEmpty())
            return retval;

        //If group by is in the responses, we need to add it before updating
        for(SelectResponse selectResponse : responses) {
            if(selectResponse == null) continue;
            for(int index : selectResponse.rows.keySet()) {
                if(index < 0) {
                    HashMap<String, Object> columns = selectResponse.rows.get(index);
                    //int groupByIndex = (int)columns.entrySet().stream().findFirst().get().getValue();

                    for(Map.Entry<String, Object> entry : columns.entrySet()) {
                        if(!selectBody.getGroupBy().equals(entry.getKey())) {
                            columns.put(entry.getKey(), Double.NaN);
                        }
                    }

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


        if(!selectBody.getRequesterIp().equals(selectBody.getCurrentIp())) {

            return this;
        }

        if(selectBody.hasGroupBy()) {

            HashMap<Integer, HashMap<String, Object>> rowsCopy = new HashMap<>(rows);

            HashMap<Integer, HashMap<String, Object>> groupedRows = rows.entrySet().stream()
                    .filter(entry -> entry.getKey() < 0)
                    .collect(HashMap::new,
                            (map, entry) -> map.put(entry.getKey(), new HashMap<>(entry.getValue())),
                            HashMap::putAll);

            HashMap<Integer, HashMap<String, Object>> finalRows = new HashMap<>();

            for (int groupKey : groupedRows.keySet()) {
                HashMap<String, Object> currentGroup = groupedRows.get(groupKey);
                int groupByIndex = -1;
                Object res = currentGroup.get(selectBody.getGroupBy());

                for (String column : currentGroup.keySet()) {
                    Object columnValue = currentGroup.get(column);

                    if (!(columnValue instanceof Double && Double.isNaN((Double) columnValue))) {
                        if (groupByIndex == -1) {
                            for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                    groupByIndex = entry.getKey();
                                    break;
                                }
                            }

                            if (groupByIndex == -1) {
                                break;
                            }
                        }

                        HashMap<String, Object> sourceGroup = rowsCopy.get(groupByIndex);

                        if (selectBody.hasSumAggregate()) {

                            for (String sumColumn : selectBody.getAggregate().getSum()) {

                                String columnName = "sum-" + sumColumn;
                                double sum = 0;

                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {

                                        if(entry.getValue().get(sumColumn) instanceof Number number) {

                                            sum+=number.doubleValue();
                                        }

                                    }
                                }

                                sourceGroup.put(columnName, sum);
                            }
                        }

                        finalRows.put(groupKey, sourceGroup);
                    }
                }
            }

            rows = finalRows;
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
