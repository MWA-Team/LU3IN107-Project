package fr.su.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Database {

    /**
     * Les différentes colonnes sont dispersées sur plusieurs serveurs. Pourquoi les colonnes ? Toutes les colonnes auront la même taille dans une table (normalement) ce qui accentue le bon partage des tâches entre serveurs
     *
     * Dans une table il y a des colonnes
     * Dans des colonnes il y a des variables
     *

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

        System.out.println("Added " + table + " to the tables");
        this.tables.put(table.getName(), table);
    }

}
