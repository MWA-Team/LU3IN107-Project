package fr.su.memorydb.database;

import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class Column<T> {

    private final Table table;
    private final String name;
    private final boolean stored;
    private Class<T> type;
    private LambdaInsertion lambda;
    private LambdaTypeConverter<T> converter;

    private final HashMap<T, byte[]> rows;

    public Column() {
        name = "Injected";
        stored = true;
        rows = new HashMap<>();
        converter = (String o) -> null;
        table = null;
    }

    public Column(Table table, String name, boolean stored, Class<T> type) {
        this.table = table;
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

    public int[] getAllIndexes() {
        int[] indexes = new int[table.getRowsCounter()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        return indexes;
    }

    public T getValue(Integer index) throws IOException {
        for (Map.Entry<T, byte[]> row : rows.entrySet()) {
            for (int tmpIndex : Snappy.uncompressIntArray(row.getValue())) {
                if (tmpIndex == index)
                    return type.cast(row.getKey());
            }
        }
        return null;
    }

    public HashMap<T, byte[]> getRows() { return rows; }

    public void addRows(T val, List<Integer> indexes) throws IOException {
        byte[] row = rows.get(val);
        if (row == null)
            rows.put(val, Snappy.compress(indexes.stream().mapToInt(Integer::intValue).toArray()));
        else {
            int[] tmp = Snappy.uncompressIntArray(row);
            for (int i : tmp) {
                indexes.add(i);
            }
            rows.put(val, Snappy.compress((indexes.stream()).mapToInt(Integer::intValue).toArray()));
        }
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

    public LambdaInsertion getLambda() {
        return lambda;
    }

}
