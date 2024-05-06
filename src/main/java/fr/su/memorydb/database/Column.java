package fr.su.memorydb.database;

import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;

import java.math.BigInteger;
import java.util.*;

public class Column<T> {

    private final String name;
    private final boolean stored;
    private Class<T> type;
    private LambdaInsertion lambda;
    private LambdaTypeConverter<T> converter;

    private final HashMap<T, HashSet<Integer>> rows; // Linked list are good because we don't access specific indexes and insert data a lot

    public Column() {
        name = "Injected";
        stored = true;
        rows = new HashMap<>();
        converter = (String o) -> null;
    }

    public Column(String name, boolean stored, Class<T> type) {
        this.name = name;
        this.stored = stored;
        this.rows = new HashMap<>();
        this.type = type;

        // Type management
        if (type.equals(Boolean.class)) {
            lambda = GroupValueSource::getBoolean;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Boolean.parseBoolean(o)) : null;
        } else if (type.equals(Integer.class)) {
            lambda = GroupValueSource::getInteger;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Integer.parseInt(o)) : null;
        } else if (type.equals(Long.class)) {
            lambda = GroupValueSource::getLong;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Long.parseLong(o)) : null;
        } else if (type.equals(BigInteger.class)) {
            lambda = GroupValueSource::getInt96;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(BigInteger.valueOf(Long.parseLong(o))) : null;
        } else if (type.equals(Float.class)) {
            lambda = GroupValueSource::getFloat;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Float.parseFloat(o)) : null;
        } else if (type.equals(Double.class)) {
            lambda = GroupValueSource::getDouble;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Double.parseDouble(o)) : null;
        } else {
            lambda = (Group g, String field, int index) -> {
                int i = g.getType().getFieldIndex(field);
                return g.getValueToString(i, index);
            };
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(o) : null;
        }
    }

    public String getName() {
        return name;
    }

    public Set<T> getValues() {
        return rows.keySet();
    }

    public HashSet<Integer> getAllIndexes() {
        HashSet<Integer> retval = new HashSet<>();
        for (HashSet<Integer> row : rows.values()) {
            retval.addAll(row);
        }
        return retval;
    }

    public T getValue(Integer index) {
        for (Map.Entry<T, HashSet<Integer>> row : rows.entrySet()) {
            if (row.getValue().contains(index))
                return row.getKey();
        }
        return null;
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

    public LambdaTypeConverter<T> getConverter() {
        return converter;
    }

}
