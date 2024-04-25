package fr.su.database;

import java.util.HashMap;

public class Column<T> {

    //Cassandra
    //orienté colonne vs orienté ligne

    private String name;
    private boolean stored;

    private HashMap<Long, T> values; //Liste des valeurs dans cette colonne (vide si c'est pas serverIdentifier == serverActuel sinon contient les données)

    public Column(String name, T type, boolean stored) {
        this.name = name;
        this.values = new HashMap<Long, T>();
        this.stored = stored;
    }

    public String getName() {
        return name;
    }

    public HashMap<Long, T> getValues() {
        return values;
    }

    public void addValue(long index, T val) {
        //System.out.println("Added value at index " + index + " with value " + val + " in column " + name);
        this.values.put(index, val);
    }

    public boolean stored() {
        return stored;
    }
}
