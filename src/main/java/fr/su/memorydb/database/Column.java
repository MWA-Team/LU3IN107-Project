package fr.su.memorydb.database;

import fr.su.memorydb.utils.lambda.LambdaCompressValues;
import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import fr.su.memorydb.utils.lambda.LambdaUncompressValues;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class Column<T> {

    private final Table table;
    private final String name;
    private final boolean stored;
    private final HashMap<T, byte[]> rows;
    private T[] values = null;

    private Class<T> type;
    private LambdaInsertion lambda;
    private LambdaTypeConverter<T> converter;
    private LambdaCompressValues lambdaCompressValues;
    private LambdaUncompressValues lambdaUncompressValues;

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
            /*lambdaCompressValues = (Object[] array) -> {
                byte[] byteArray = new byte[array.length];
                for (int i = 0; i < array.length; i++) {
                    byteArray[i] = (byte) ((Boolean) array[i] ? 1 : 0);
                }
                return byteArray;
            };
            lambdaUncompressValues = (byte[] array) -> {
                Boolean[] uncompressedArray = new Boolean[array.length];
                for (int i = 0; i < array.length; i++) {
                    uncompressedArray[i] = array[i] == 1 ? Boolean.TRUE : Boolean.FALSE;
                }
                return uncompressedArray;
            };*/
        } else if (type.equals(Integer.class)) {
            lambda = GroupValueSource::getInteger;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Integer.parseInt(o)) : null;
            /*lambdaCompressValues = (Object[] array) -> {
                int[] primitiveArray = new int[array.length];
                for (int i = 0; i < array.length; i++) {
                        primitiveArray[i] = array[i] != null ? (Integer) array[i] : null;
                }
                return Snappy.compress(primitiveArray);
            };
            lambdaUncompressValues = (byte[] array) -> {
                int[] uncompressedArray = Snappy.uncompressIntArray(array);
                return Arrays.stream(uncompressedArray).mapToObj(value -> (Integer) value).toArray();
            };*/
        } else if (type.equals(Long.class)) {
            lambda = GroupValueSource::getLong;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Long.parseLong(o)) : null;
            /*lambdaCompressValues = (Object[] array) -> {
                long[] primitiveArray = new long[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (Long) array[i];
                }
                return Snappy.compress(primitiveArray);
            };
            lambdaUncompressValues = (byte[] array) -> {
                long[] uncompressedArray = Snappy.uncompressLongArray(array);
                return Arrays.stream(uncompressedArray).mapToObj(value -> (Long) value).toArray();
            };*/
        } else if (type.equals(BigInteger.class)) {
            lambda = GroupValueSource::getInt96;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(BigInteger.valueOf(Long.parseLong(o))) : null;
            /*lambdaCompressValues = (Object[] array) -> {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                try (DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)) {
                    for (Object bigInt : array) {
                        byte[] strBytes = ((BigInteger) bigInt).toString().getBytes();
                        deflaterOutputStream.write(strBytes.length >> 8);
                        deflaterOutputStream.write(strBytes.length);
                        deflaterOutputStream.write(strBytes);
                    }
                }
                return byteArrayOutputStream.toByteArray();
            };
            lambdaUncompressValues = (byte[] array) -> {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(array);
                try (InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream)) {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    int len;
                    while ((len = inflaterInputStream.read()) != -1) {
                        len <<= 8;
                        len |= inflaterInputStream.read();
                        byte[] buffer = new byte[len];
                        inflaterInputStream.read(buffer);
                        byteArrayOutputStream.write(buffer);
                    }
                    String[] strings = byteArrayOutputStream.toString().split("\0");
                    // TODO
                    return null;
                }
            };*/
        } else if (type.equals(Float.class)) {
            lambda = GroupValueSource::getFloat;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Float.parseFloat(o)) : null;
            /*lambdaCompressValues = (Object[] array) -> {
                float[] primitiveArray = new float[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (Float) array[i];
                }
                return Snappy.compress(primitiveArray);
            };
            lambdaUncompressValues = (byte[] array) -> {
                float[] uncompressedArray = Snappy.uncompressFloatArray(array);
                Float[] floatArray = new Float[uncompressedArray.length];
                for (int i = 0; i < uncompressedArray.length; i++) {
                    floatArray[i] = uncompressedArray[i];
                }
                return floatArray;
            };*/
        } else if (type.equals(Double.class)) {
            lambda = GroupValueSource::getDouble;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Double.parseDouble(o)) : null;
           /*lambdaCompressValues = (Object[] array) -> {
                double[] primitiveArray = new double[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (Double) array[i];
                }
                return Snappy.compress(primitiveArray);
            };
            lambdaUncompressValues = (byte[] array) -> {
                double[] uncompressedArray = Snappy.uncompressDoubleArray(array);
                return Arrays.stream(uncompressedArray).mapToObj(value -> (Double) value).toArray();
            };*/
        } else {
            lambda = (Group g, String field, int index) -> {
                int i = g.getType().getFieldIndex(field);
                return g.getValueToString(i, index);
            };
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(o) : null;
            /*lambdaCompressValues = (Object[] array) -> {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                try (DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)) {
                    for (Object str : array) {
                        byte[] strBytes = str.toString().getBytes();
                        deflaterOutputStream.write(strBytes.length >> 8);
                        deflaterOutputStream.write(strBytes.length);
                        deflaterOutputStream.write(strBytes);
                    }
                }
                return byteArrayOutputStream.toByteArray();
            };
            lambdaUncompressValues = (byte[] array) -> {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(array);
                try (InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream)) {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    int len;
                    while ((len = inflaterInputStream.read()) != -1) {
                        len <<= 8;
                        len |= inflaterInputStream.read();
                        byte[] buffer = new byte[len];
                        inflaterInputStream.read(buffer);
                        byteArrayOutputStream.write(buffer);
                    }
                    return byteArrayOutputStream.toString().split("\0");
                }
            };*/
        }
    }

    public String getName() {
        return name;
    }

    public Set<T> getValues() {
        return rows.keySet();
    }

    public int[] getAllIndexes() {
        int[] array = new int[table.rowsCounter];
        for (int i = 0; i < table.rowsCounter; i++)
            array[i] = i;
        return array;
    }

    public int[] get(T val) throws IOException {
        byte[] indexes = rows.get(val);
        if (indexes == null)
            return null;
        return Snappy.uncompressIntArray(indexes);
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
        if (row == null) {
            int[] array = indexes.stream().mapToInt(Integer::intValue).toArray();
            Arrays.parallelSort(array);
            rows.put(val, Snappy.compress(array));
        } else {
            int[] tmp = Snappy.uncompressIntArray(row);
            for (int i : tmp) {
                indexes.add(i);
            }
            int[] array = indexes.stream().mapToInt(Integer::intValue).toArray();
            Arrays.parallelSort(array);
            rows.put(val, Snappy.compress(array));
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

    public void initValues() throws IOException {
        values = (T[]) Array.newInstance(type, table.getRowsCounter());
        for (Map.Entry<T, byte[]> row : rows.entrySet()) {
            int[] indexes = Snappy.uncompressIntArray(row.getValue());
            for (int i = 0; i < indexes.length; i++) {
                T val = row.getKey();
                values[indexes[i]] = val;
            }
        }
    }

    public T[] getValuesAsArray() throws IOException {
        return values;
    }

}
