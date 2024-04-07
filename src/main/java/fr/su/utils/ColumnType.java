package fr.su.utils;

import java.util.Date;

public enum ColumnType {

    STRING(String.class, "String"),
    INTEGER(Integer.class, "int"),
    DATE(Date .class, "Date");

    private Class<?> type;
    private String identifier;

    ColumnType(Class<?> type, String identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    public Class<?> getType() {
        return type;
    }

    public String getIdentifier() {
        return identifier;
    }
}
