package fr.su.database;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Column<T> {

    //Cassandra
    //orienté colonne vs orienté ligne

    private UUID serverIdentifier; //Où est située cette colonne ?

    private String name; //Nom de la colonne

    private HashMap<Integer, T> values; //Liste des valeurs dans cette colonne (vide si c'est pas serverIdentifier == serverActuel sinon contient les données)

    public Column(String name, T type) {

        this.name = name;
        this.values = new HashMap<Integer, T>();
    }

    public UUID getServerIdentifier() {
        return serverIdentifier;
    }

    public String getName() {
        return name;
    }

    public HashMap<Integer, T> getValues() {
        return values;
    }
}
