package fr.su.database;

import java.util.*;

public class Column<T> {

    private String name;
    private boolean stored;

    private HashMap<T, HashSet<Long>> rows; // Linked list are good because we don't access specific indexes and insert data a lot

    public Column() {
        name = "Injected";
        stored = true;
        rows = new HashMap<>();
    }

    public Column(String name, boolean stored) {
        this.name = name;
        this.stored = stored;
        this.rows = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public Set<T> getValues() {
        return rows.keySet();
    }

    public HashMap<T, HashSet<Long>> getRows() { return rows; }

    public void addRow(T val, long index) {
        HashSet<Long> row = rows.computeIfAbsent(val, k -> new HashSet<>());
        row.add(index);
    }

    public boolean stored() {
        return stored;
    }
}
