package fr.su.database;

import java.util.List;
import java.util.UUID;

public class Database {

    /**
     * Les différentes colonnes sont dispersées sur plusieurs serveurs. Pourquoi les colonnes ? Toutes les colonnes auront la même taille dans une table (normalement) ce qui accentue le bon partage des tâches entre serveurs
     *
     * Dans une table il y a des colonnes
     * Dans des colonnes il y a des variables
     */

    private String name;
    private UUID uuid;
    private List<Table> tables;
}
