package fr.su.handlers.select.response;

import fr.su.database.Column;

import java.util.*;

public class SelectResponse {

    /*
    This class is used to assemble the response we need to send to the requester.
    It will get all the columns sorted by WHERE clause and
     */


    private Set<Integer> indexes;
    private List<Column> columns;

    public SelectResponse() {
        this.indexes = new HashSet<>();
        this.columns = new ArrayList<>();
    }

    public Set<Integer> getIndexes() { return indexes; }

    public List<Column> getColumns() { return columns; }

    public SelectResponse merge(List<SelectResponse> selectResponse) {
        SelectResponse retval = new SelectResponse();
        selectResponse.add(this);

        retval.getIndexes().addAll(indexes);

        for (SelectResponse response : selectResponse) {
            if (response == null)
                continue;
            retval.columns.addAll(response.columns);
        }

        // Adding rows
        for (Integer index : this.indexes) {

            for (SelectResponse response : selectResponse) {
                if (response == null)
                    continue;
                if (!response.indexes.contains(index)) {
                    retval.indexes.remove(index);
                }
            }

        }

        return retval;
    };

}
