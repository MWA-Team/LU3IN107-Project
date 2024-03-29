package fr.su.database;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Table {

    private String name;

    private HashMap<UUID, Column> columns; //Columns by server

    public Table(String name) {
        this.name = name;
    }

    public String getName() { return this.name; }

    public HashMap<UUID, Column> getColumns() {
        return columns;
    }
}
