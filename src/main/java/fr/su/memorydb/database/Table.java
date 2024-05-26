package fr.su.memorydb.database;

import java.util.*;

public class Table {

    private String name;

    private HashMap<String, Column> columnsNames;
    private List<Column> columns;

    public int rowsCounter;

    public Table(String name) {
        this.name = name;
        rowsCounter = 0;
        this.columnsNames = new HashMap<>();
        columns = new ArrayList<>();
    }

    public String getName() { return this.name; }

    public HashMap<String, Column> getColumnsNames() {
        return columnsNames;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(String name) {
        return columnsNames.get(name);
    }

    public void addColumn(Column column) {
        this.columns.add(column);
        this.columnsNames.put(column.getName(), column);
    }

    public void setRowsCounter(int rowsCounter) {
        this.rowsCounter = rowsCounter;
    }

    public int getRowsCounter() {
        return rowsCounter;
    }

}
