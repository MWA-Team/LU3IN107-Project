package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class WhereResponse {

    @JsonProperty
    private String table;

    @JsonProperty
    private int[] indexes;

    public WhereResponse() {}

    public WhereResponse(String table, int[] indexes) {
        this.table = table;
        this.indexes = indexes;
    }

    public String getTable() {
        return table;
    }

    public int[] getIndexes() {
        return indexes;
    }

    public void setIndexes(int[] indexes) {
        this.indexes = indexes;
    }

    public int[] mergeIndexes(List<int[]> responses) throws InterruptedException {
        if (responses == null || responses.isEmpty())
            return this.indexes;

        List<Integer> mergedIndexes = new LinkedList<>();
        for (int index : indexes) {
            boolean pass = false;
            for (int[] tmp : responses) {
                if (tmp == null)
                    continue;
                
                int found = Arrays.binarySearch(tmp, index);
                if (found < 0) {
                    pass = true;
                    break;
                }
            }

            if (!pass)
                mergedIndexes.add(index);
        }

        return mergedIndexes.stream().mapToInt(Integer::intValue).toArray();
    }

}
