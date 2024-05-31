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

                        int globalCount = -1; //optimize mean and count

                        //Managing all sum aggregates
                        if (selectBody.hasSumAggregate()) {
                            for (String sumColumn : selectBody.getAggregate().getSum()) {
                                String columnName = "sum-" + sumColumn;
                                double sum = 0;
                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                        if(entry.getValue().get(sumColumn) instanceof Number number) sum+=number.doubleValue();
                                    }
                                }
                                sourceGroup.put(columnName, sum);
                            }
                        }

                        //Managing all count aggregates
                        if (selectBody.hasCountAggregate()) {
                            for (String countColumn : selectBody.getAggregate().getCount()) {
                                String columnName = "count-" + countColumn;
                                int count = 0;
                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                        count+=1;
                                    }
                                }
                                sourceGroup.put(columnName, count);
                                globalCount = count;
                            }
                        }

                        //Managing all max aggregates
                        if (selectBody.hasMaxAggregate()) {
                            for (String maxColumn : selectBody.getAggregate().getMax()) {
                                String columnName = "max-" + maxColumn;
                                double max = 0;

                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                        if(entry.getValue().get(maxColumn) instanceof Number number && (Double.isNaN(max) || number.doubleValue() > max)) {
                                            max = number.doubleValue();
                                        }
                                    }
                                }
                                sourceGroup.put(columnName, max);
                            }
                        }

                        //Managing all min aggregates
                        if (selectBody.hasMaxAggregate()) {
                            for (String maxColumn : selectBody.getAggregate().getMax()) {
                                String columnName = "min-" + maxColumn;
                                double min = Double.NaN;

                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                        if(entry.getValue().get(maxColumn) instanceof Number number && (Double.isNaN(min) || number.doubleValue() < min)) {
                                            min = number.doubleValue();
                                        }
                                    }
                                }
                                sourceGroup.put(columnName, min);
                            }
                        }

                        //Managing all mean aggregates
                        if (selectBody.hasMeanAggregate()) {
                            for (String meanColumn : selectBody.getAggregate().getMean()) {
                                String columnName = "mean-" + meanColumn;
                                double sum = 0;
                                int count = 0;

                                for (Map.Entry<Integer, HashMap<String, Object>> entry : rowsCopy.entrySet()) {
                                    if (entry.getKey() >= 0 && Objects.equals(entry.getValue().get(selectBody.getGroupBy()), res)) {
                                        if(entry.getValue().get(meanColumn) instanceof Number number) {
                                            sum = number.doubleValue();
                                            count++;
                                        }
                                    }
                                }
                                sourceGroup.put(columnName, (float)sum/(float)count);
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
