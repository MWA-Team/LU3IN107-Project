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

    /*public SelectResponse aggregate(TableSelection.SelectBody selectBody) {
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
    }*/

}
