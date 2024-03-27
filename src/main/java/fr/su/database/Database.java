package fr.su.database;

import java.util.ArrayList;
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
        //Meme démarche vu en cours
        // pour garantir qu'une seule instance de la base de données est créée
        // même dans un environnement multi-threadé.
    }

    private List<Table> tables;

    private Database() {
        this.tables = new ArrayList<>();
    }

    public static Database getInstance() {
        return Instance.INSTANCE;
    }

    public List<Table> getTables() {
        return new ArrayList<>(tables);
    }

    public Table getTableByIndex(int i) {
        //utilité pas encore claire mais
        return tables.get(i);
    }

}
