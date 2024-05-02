package fr.su.database;

import fr.su.handlers.insertion.LambdaInsertion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;

import java.math.BigInteger;
import java.util.*;

public class Column<T> {

    private final String name;
    private final boolean stored;
    private Class<T> type;
    private LambdaInsertion lambda;

    private final HashMap<T, HashSet<Integer>> rows; // Linked list are good because we don't access specific indexes and insert data a lot

    public Column() {
        name = "Injected";
        stored = true;
        rows = new HashMap<>();
    }

    public Column(String name, boolean stored, Class<T> type) {
        this.name = name;
        this.stored = stored;
        this.rows = new HashMap<>();
        this.type = type;

        // Type management
        if (type.equals(Boolean.class)) {
            lambda = (Group g, String field, int index) -> g.getBoolean(field, index);
        } else if (type.equals(Integer.class)) {
            lambda = (Group g, String field, int index) -> g.getInteger(field, index);
        } else if (type.equals(Long.class)) {
            lambda = (Group g, String field, int index) -> g.getLong(field, index);
        } else if (type.equals(BigInteger.class)) {
            lambda = (Group g, String field, int index) -> g.getInt96(field, index);
        } else if (type.equals(Float.class)) {
            lambda = (Group g, String field, int index) -> g.getFloat(field, index);
        } else if (type.equals(Double.class)) {
            lambda = (Group g, String field, int index) -> g.getDouble(field, index);
        } else {
            lambda = (Group g, String field, int index) -> {
                int i = g.getType().getFieldIndex(field);
                return g.getValueToString(i, index);
            };
        }
    }

    public String getName() {
        return name;
    }

    public Set<T> getValues() {
        return rows.keySet();
    }

    public HashMap<T, HashSet<Integer>> getRows() { return rows; }

    public void addRowGroup(Group g, String field, int index) {
        T val = type.cast(lambda.call(g, field, 0));
        HashSet<Integer> row = rows.computeIfAbsent(val, k -> new HashSet<>());
        row.add(index);
    }

    public void addRowValue(T val, int index) {
        HashSet<Integer> row = rows.computeIfAbsent(val, k -> new HashSet<>());
        row.add(index);
    }

    public boolean stored() {
        return stored;
    }

    public Class<T> getType() {
        return type;
    }

}
