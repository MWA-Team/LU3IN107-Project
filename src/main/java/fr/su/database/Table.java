package fr.su.database;

import java.util.*;

public class Table {

    private String name;

    private HashMap<String, Column> columns;

    public Table(String name) {
        this.name = name;
        this.columns = new HashMap<>();
    }

    public String getName() { return this.name; }

    public HashMap<String, Column> getColumns() {
        return columns;
    }

}
