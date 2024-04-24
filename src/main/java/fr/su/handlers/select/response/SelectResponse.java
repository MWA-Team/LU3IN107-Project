package fr.su.handlers.select.response;

import fr.su.database.Column;

import java.util.*;

public class SelectResponse {

    /*
    This class is used to assemble the response we need to send to the requester.
    It will get all the columns sorted by WHERE clause and
     */


    private int status;
    private Set<Integer> indexes;
    private List<Column> columns;

    public SelectResponse() {

        this.status = 400;
        this.indexes = new HashSet<>();
        this.columns = new ArrayList<>();
    }

    public Set<Integer> getIndexes() { return indexes; }

    public List<Column> getColumns() { return columns; }

    public SelectResponse merge(SelectResponse selectResponse) {


        //TODO
        return this;
    };

}
