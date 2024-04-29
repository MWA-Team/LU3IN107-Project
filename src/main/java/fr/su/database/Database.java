package fr.su.database;

import java.util.HashMap;

public class Database {

    /**
    private String name;
    private UUID uuid;
     */

    private static class Instance{
        private static final Database INSTANCE = new Database();
    }

    private HashMap<String, Table> tables;

    private Database() {
        this.tables = new HashMap<>();
    }

    public static Database getInstance() {
        return Instance.INSTANCE;
    }

    public HashMap<String, Table> getTables() {
        return new HashMap<>(tables);
    }

    public void addTable(Table table) {
        this.tables.put(table.getName(), table);
    }

}
