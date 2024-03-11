package fr.su.database;

import java.util.List;
import java.util.UUID;

public class Column<T> {

    private UUID serverIdentifier; //Où est située cette colonne ?

    private String name; //Nom de la colonne
    private T columnType; //Type de variable dans cette colonne

    private List<T> values; //Liste des valeurs dans cette colonne (vide si c'est pas serverIdentifier == serverActuel sinon contient les données)





}
