package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.controllers.TableSelection;

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

    public static List<HashMap<String, Object>> mergeRows(List<List<HashMap<String, Object>>> rows, TableSelection.SelectBody selectBody) {
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

        // Getting the indexes from the potential "group by"
        List<List<Integer>> indexes = sortIndexes(selectBody.getGroupBy(), rows, null);

        TableSelection.Aggregate aggregate = selectBody.getAggregate();

        // Actually merging the rows
        List<HashMap<String, Object>> mergedRows = new ArrayList<>(indexes == null ? length : indexes.size());
        if (indexes == null) {
            for (int i = 0; i < length; i++) {
                for (List<HashMap<String, Object>> tmpRows : rows) {
                    if (tmpRows == null)
                        continue;
                    HashMap<String, Object> mergedRow;
                    if (mergedRows.size() <= i) {
                        mergedRow = tmpRows.get(i);
                        mergedRows.add(mergedRow);
                    } else {
                        mergedRow = mergedRows.get(i);
                        mergedRow.putAll(tmpRows.get(i));
                    }

                    if (selectBody.hasMeanAggregate() && tmpRows.get(0) != null) {
                        for (String column : aggregate.getMean()) {
                            if (!tmpRows.get(0).containsKey(column))
                                continue;
                            aggregate.mean(tmpRows, column, null, mergedRow);
                        }
                    }
                    if (selectBody.hasSumAggregate() && tmpRows.get(0) != null) {
                        for (String column : aggregate.getMean()) {
                            if (!tmpRows.get(0).containsKey(column))
                                continue;
                            aggregate.sum(tmpRows, column, null, mergedRow);
                        }
                    }
                    if (selectBody.hasCountAggregate() && tmpRows.get(0) != null) {
                        for (String column : aggregate.getMean()) {
                            if (!tmpRows.get(0).containsKey(column))
                                continue;
                            aggregate.count(tmpRows, column, null, mergedRow);
                        }
                    }
                    if (selectBody.hasMaxAggregate() && tmpRows.get(0) != null) {
                        for (String column : aggregate.getMax()) {
                            if (!tmpRows.get(0).containsKey(column))
                                continue;
                            aggregate.max(tmpRows, column, null, mergedRow);
                        }
                    }
                    if (selectBody.hasMinAggregate() && tmpRows.get(0) != null) {
                        for (String column : aggregate.getMin()) {
                            if (!tmpRows.get(0).containsKey(column))
                                continue;
                            aggregate.min(tmpRows, column, null, mergedRow);
                        }
                    }
                    if (aggregate != null)
                        break;
                }
                if (aggregate != null)
                    break;
            }
        } else {
            int size = 0;
            for (List<Integer> listIndexes : indexes) {
                for (Integer index : listIndexes) {
                    size++;
                    for (List<HashMap<String, Object>> tmpRows : rows) {
                        if (tmpRows == null || tmpRows.isEmpty())
                            continue;
                        HashMap<String, Object> mergedRow;
                        if (mergedRows.size() < size) {
                            mergedRow = tmpRows.get(index);
                            mergedRows.add(mergedRow);
                        } else {
                            mergedRow = mergedRows.get(size - 1);
                            mergedRow.putAll(tmpRows.get(index));
                        }
                        if (selectBody.hasMeanAggregate() && tmpRows.get(0) != null) {
                            for (String column : aggregate.getMean()) {
                                if (!tmpRows.get(0).containsKey(column))
                                    continue;
                                aggregate.mean(tmpRows, column, listIndexes, mergedRow);
                            }
                        }
                        if (selectBody.hasSumAggregate() && tmpRows.get(0) != null) {
                            for (String column : aggregate.getSum()) {
                                if (!tmpRows.get(0).containsKey(column))
                                    continue;
                                aggregate.sum(tmpRows, column, listIndexes, mergedRow);
                            }
                        }
                        if (selectBody.hasCountAggregate() && tmpRows.get(0) != null) {
                            for (String column : aggregate.getCount()) {
                                if (!tmpRows.get(0).containsKey(column))
                                    continue;
                                aggregate.count(tmpRows, column, listIndexes, mergedRow);
                            }
                        }
                        if (selectBody.hasMaxAggregate() && tmpRows.get(0) != null) {
                            for (String column : aggregate.getMax()) {
                                if (!tmpRows.get(0).containsKey(column))
                                    continue;
                                aggregate.max(tmpRows, column, listIndexes, mergedRow);
                            }
                        }
                        if (selectBody.hasMinAggregate() && tmpRows.get(0) != null) {
                            for (String column : aggregate.getMin()) {
                                if (!tmpRows.get(0).containsKey(column))
                                    continue;
                                aggregate.min(tmpRows, column, listIndexes, mergedRow);
                            }
                        }
                    }
                    break;
                }
            }
        }
        return mergedRows;
    }

    public static List<HashMap<String, Object>> naiveMergeRows(List<List<HashMap<String, Object>>> rows) {
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

        List<HashMap<String, Object>> mergedRows = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            for (List<HashMap<String, Object>> tmpRows : rows) {
                if (tmpRows == null)
                    continue;
                HashMap<String, Object> mergedRow;
                if (mergedRows.size() <= i) {
                    mergedRow = tmpRows.get(i);
                    mergedRows.add(mergedRow);
                } else {
                    mergedRow = mergedRows.get(i);
                    mergedRow.putAll(tmpRows.get(i));
                }
            }
        }

        return mergedRows;
    }

    private static List<List<Integer>> sortIndexes(List<String> groupBy, List<List<HashMap<String, Object>>> rows, List<List<Integer>> sortedIndexes) {
        if (rows == null || rows.isEmpty() || groupBy == null)
            return null;

        if (groupBy.isEmpty())
            return sortedIndexes;

        List<List<Integer>> retval = new LinkedList<>();
        String column = groupBy.remove(groupBy.size() - 1);
        if (sortedIndexes == null) {
            // First call
            HashMap<Object, List<Integer>> map = new HashMap<>();
            for (List<HashMap<String, Object>> tmpRows : rows) {
                if (tmpRows == null)
                    continue;
                boolean pass = false;
                for (int i = 0; i < tmpRows.size(); i++) {
                    HashMap<String, Object> tmpRow = tmpRows.get(i);
                    if (!tmpRow.containsKey(column)) {
                        // Then this whole list does not contain this column, so we can skip it for our group by
                        pass = true;
                        break;
                    }
                    List<Integer> indexes = map.computeIfAbsent(tmpRow.get(column), k -> new LinkedList<>());
                    indexes.add(i);
                }
                if (pass)
                    continue;
                // If we get here, it means that we parsed a list containing the column to sort on, so we can exit the loop as the others won't have it either way
                break;
            }

            // Adding the sorted indexes to the list
            for (Map.Entry<Object, List<Integer>> entry : map.entrySet()) {
                retval.add(entry.getValue());
            }
        } else {
            // Following calls
            for (List<HashMap<String, Object>> tmpRows : rows) {
                if (tmpRows == null)
                    continue;

                boolean pass = false;
                for (List<Integer> listIndexes : sortedIndexes) {
                    HashMap<Object, List<Integer>> map = new HashMap<>();
                    for (Integer index : listIndexes) {
                        HashMap<String, Object> tmpRow = tmpRows.get(index);
                        if (!tmpRow.containsKey(column)) {
                            // Then this whole list does not contain this column, so we can skip it for our group by
                            pass = true;
                            break;
                        }
                        List<Integer> indexes = map.computeIfAbsent(tmpRow.get(column), k -> new LinkedList<>());
                        indexes.add(index);
                    }
                    if (pass)
                        break;

                    // Adding the sorted indexes to the list
                    for (Map.Entry<Object, List<Integer>> entry : map.entrySet()) {
                        retval.add(entry.getValue());
                    }
                }
            }
        }

        return sortIndexes(groupBy, rows, retval);
    }

}

