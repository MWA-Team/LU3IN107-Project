package fr.su.memorydb.database;

import fr.su.memorydb.utils.compress.Compressor;
import fr.su.memorydb.utils.compress.NoOpCompressor;
import fr.su.memorydb.utils.compress.SnappyCompressor;
import fr.su.memorydb.utils.lambda.LambdaCompress;
import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import fr.su.memorydb.utils.lambda.LambdaUncompress;
import fr.su.memorydb.utils.streams.inputstreams.*;
import fr.su.memorydb.utils.streams.outputstreams.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class Column<T> {

    private final Table table;
    private final String name;
    private final boolean stored;
    private int blocsSize;
    private List<HashMap<T, Object>> rows;
    private final List<Object> values;
    private final Class<T> type;
    private LambdaInsertion lambda;
    private final LambdaTypeConverter<T> converter;
    private final Compressor valuesCompressor;
    private final Compressor indexesCompressor;
    private boolean enableIndexing;
    private boolean lastBlocIsFull = true;

    public Column() {
        name = null;
        stored = false;
        blocsSize = 0;
        rows = null;
        converter = null;
        table = null;
        values = null;
        type = null;
        valuesCompressor = null;
        indexesCompressor = null;
        enableIndexing = false;
    }

    public Column(Table table, String name, boolean stored, int blocsSize, Class<T> type, boolean enableValuesCompression, boolean enableIndexesCompression, boolean enableIndexing) {
        this.table = table;
        this.name = name;
        this.stored = stored;
        this.blocsSize = blocsSize;
        this.rows = new ArrayList<>();
        this.type = type;
        this.values = new ArrayList<>();
        this.enableIndexing = enableIndexing;

        LambdaCompress lambdaCompressValues = null;
        LambdaUncompress lambdaUncompressValues = null;

        // Type management
        if (type.equals(Boolean.class)) {
            lambda = GroupValueSource::getBoolean;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Boolean.parseBoolean(o)) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    BooleanOutputStream out = new BooleanOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeBoolean((Boolean) Array.get(array, i));
                    }
                    out.finish();
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    BooleanInputStream in = new BooleanInputStream(array);
                    LinkedList<Boolean> uncompressedList = new LinkedList<>();
                    int i = 0;
                    while (true) {
                        try {
                            uncompressedList.add(in.readBoolean());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        } else if (type.equals(Integer.class)) {
            lambda = GroupValueSource::getInteger;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Integer.parseInt(o)) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    IntegerOutputStream out = new IntegerOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeInteger((Integer) Array.get(array, i));
                    }
                    out.finish();
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    IntegerInputStream in = new IntegerInputStream(array);
                    LinkedList<Integer> uncompressedList = new LinkedList<>();
                    while (true) {
                        try {
                            uncompressedList.add(in.readInteger());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        } else if (type.equals(Long.class)) {
            lambda = GroupValueSource::getLong;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Long.parseLong(o)) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    LongOutputStream out = new LongOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeLong((Long) Array.get(array, i));
                    }
                    out.finish();
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    LongInputStream in = new LongInputStream(array);
                    LinkedList<Long> uncompressedList = new LinkedList<>();
                    while (true) {
                        try {
                            uncompressedList.add(in.readLong());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        }  else if (type.equals(Float.class)) {
            lambda = GroupValueSource::getFloat;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Float.parseFloat(o)) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    FloatOutputStream out = new FloatOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeFloat((Float) Array.get(array, i));
                    }
                    out.finish();
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    FloatInputStream in = new FloatInputStream(array);
                    LinkedList<Float> uncompressedList = new LinkedList<>();
                    while (true) {
                        try {
                            uncompressedList.add(in.readFloat());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        } else if (type.equals(Double.class)) {
            lambda = GroupValueSource::getDouble;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Double.parseDouble(o)) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    DoubleOutputStream out = new DoubleOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeDouble((Double) Array.get(array, i));
                    }
                    out.finish();
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    DoubleInputStream in = new DoubleInputStream(array);
                    LinkedList<Double> uncompressedList = new LinkedList<>();
                    while (true) {
                        try {
                            uncompressedList.add(in.readDouble());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        } else {
            lambda = (Group g, String field, int index) -> {
                int i = g.getType().getFieldIndex(field);
                return g.getValueToString(i, index);
            };
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(o) : null;
            if (enableValuesCompression) {
                lambdaCompressValues = (Object array, int size) -> {
                    StringOutputStream out = new StringOutputStream();
                    for (int i = 0; i < size; i++) {
                        out.writeString((String) Array.get(array, i));
                    }
                    byte[] bytes = out.toByteArray();
                    out.close();
                    return bytes;
                };
                lambdaUncompressValues = (byte[] array) -> {
                    StringInputStream in = new StringInputStream(array);
                    LinkedList<Object> uncompressedList = new LinkedList<>();
                    while (true) {
                        try {
                            uncompressedList.add(in.readString());
                        } catch (IOException e) {
                            break;
                        }
                    }
                    in.close();
                    return uncompressedList.toArray();
                };
            }
        }

        this.valuesCompressor = enableValuesCompression ? new SnappyCompressor(lambdaCompressValues, lambdaUncompressValues) : new NoOpCompressor();

        if (enableIndexesCompression && enableIndexing) {
            LambdaCompress lambdaCompressIndexes = (Object array, int size) -> {
                IndexOutputStream out = new IndexOutputStream();
                for (int i = 0; i < size; i++) {
                    out.writeIndex((Integer) Array.get(array, i));
                }
                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            };
            LambdaUncompress lambdaUncompressIndexes = (byte[] array) -> {
                IndexInputStream in = new IndexInputStream(array);
                LinkedList<Integer> uncompressedList = new LinkedList<>();
                while (true) {
                    try {
                        uncompressedList.add(in.readInteger());
                    } catch (IOException e) {
                        break;
                    }
                }
                in.close();
                return uncompressedList.toArray();
            };
            this.indexesCompressor = new SnappyCompressor(lambdaCompressIndexes, lambdaUncompressIndexes);
        } else
            this.indexesCompressor = new NoOpCompressor();
    }

    public String getName() {
        return name;
    }

    public List<HashMap<T, Object>> getRows() { return rows; }

    public void addRows(Object[] newRows, int size) throws IOException {
        int limit = size > 0 ? size : newRows.length;
        Object[] toAdd;
        boolean replacing = true;

        // Getting last inserted array
        if (!lastBlocIsFull)
            toAdd = (Object[]) valuesCompressor.uncompress(values.get(values.size() - 1));
        else {
            toAdd = newRows;
            replacing = false;
        }

        if (replacing) {
            Object[] tmp = toAdd;
            int newLength = Math.min(tmp.length + limit, blocsSize);
            toAdd = new Object[newLength];
            // Adding already present values in the new array
            System.arraycopy(tmp, 0, toAdd, 0, tmp.length);
            // Completing the array with new values
            System.arraycopy(newRows, 0, toAdd, tmp.length, newLength - tmp.length);
            values.set(Math.max(values.size() - 1, 0), valuesCompressor.compress(toAdd, newLength));

            // Changing indexes if needed
            if (indexingEnabled()) {
                HashMap<T, LinkedList<Integer>> rows = new HashMap<>();
                int offset = size > 0 ? size : newRows.length;
                for (int i = 0; i < newLength; i++) {
                    LinkedList<Integer> indexes;

                    // If there was no index for this value, we create a list for that
                    if (rows.containsKey(type.cast(toAdd[i])))
                        indexes = rows.get(type.cast(toAdd[i]));
                    else {
                        indexes = new LinkedList<>();
                        rows.put(type.cast(toAdd[i]), indexes);
                    }

                    // Linking current index and this value
                    indexes.add(table.rowsCounter - offset + i);
                }

                // Adding all changes to the database
                HashMap<T, Object> newBloc = new HashMap<>();
                for (Map.Entry<T, LinkedList<Integer>> entry : rows.entrySet()) {
                    int[] array = entry.getValue().stream().mapToInt(Integer::intValue).toArray();

                    // Adding compressed indexes
                    newBloc.put(entry.getKey(), indexesCompressor.compress(array, array.length));
                }

                // Adding changes
                this.rows.set(this.rows.size() - 1, newBloc);
            }

            int max = Math.max(tmp.length - newLength, newLength - tmp.length);
            limit = limit - max;
            toAdd = new Object[limit];
            // Adding the rest of the values
            System.arraycopy(newRows, max, toAdd, 0, toAdd.length);
        }

        lastBlocIsFull = limit == blocsSize;

        // Adding compressed values to the values
        values.add(valuesCompressor.compress(toAdd, limit));

        if (!enableIndexing)
            return;

        // Creating indexes if needed
        HashMap<T, LinkedList<Integer>> rows = new HashMap<>();

        for (int i = 0; i < limit; i++) {
            LinkedList<Integer> indexes;

            // If there was no index for this value, we create a list for that
            if (rows.containsKey(type.cast(toAdd[i])))
                indexes = rows.get(type.cast(toAdd[i]));
            else {
                indexes = new LinkedList<>();
                rows.put(type.cast(toAdd[i]), indexes);
            }

            // Linking current index and this value
            indexes.add(table.rowsCounter - limit + i);
        }

        // Adding all changes to the database
        HashMap<T, Object> newBloc = new HashMap<>();
        for (Map.Entry<T, LinkedList<Integer>> entry : rows.entrySet()) {
            int[] array = entry.getValue().stream().mapToInt(Integer::intValue).toArray();

            // Adding compressed indexes
            newBloc.put(entry.getKey(), indexesCompressor.compress(array, array.length));
        }

        // Adding changes
        this.rows.add(newBloc);
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

    public int getBlocsSize() {
        return blocsSize;
    }

    public void setBlocsSize(int blocsSize) {
        this.blocsSize = blocsSize;
    }

    public int getLastBlocsSize() throws IOException {
        if (!values.isEmpty()) {
            return Array.getLength(valuesCompressor.uncompress(values.get(values.size() - 1)));
        }
        return -1;
    }

    public boolean isLastBlocIsFull() {
        return lastBlocIsFull;
    }

    public T get(int index) throws IOException {
        int count = 0;
        for (Object bloc : values) {
            Object tmp = valuesCompressor.uncompress(bloc);
            int length = Array.getLength(tmp);
            if (index <= length + count) {
                Object retval = Array.get(tmp, index - count);
                return retval == null ? null : type.cast(retval);
            } else {
                count += length;
            }
        }
        throw new IndexOutOfBoundsException();
    }

    public int[] get(T val) throws IOException {
        LinkedList<Integer> list = new LinkedList<>();

        if (enableIndexing) {
            for (HashMap<T, Object> bloc : rows) {
                Object indexes = bloc.get(val);
                if (indexes == null)
                    continue;

                // Decompressing indexes
                Object tmp = indexesCompressor.uncompress(indexes);
                for (int j = 0; j < Array.getLength(tmp); j++) {
                    list.add((Integer) Array.get(tmp, j));
                }
            }
        } else {
            // Might need to improve this part for more efficient search with threads perhaps
            int index = 0;
            for (Object bloc : values) {
                Object tmpArray = valuesCompressor.uncompress(bloc);
                for (int j = 0; j < Array.getLength(tmpArray); j++) {
                    Object tmp = Array.get(tmpArray, j);
                    if ((val == null && tmp == null) || (tmp != null && tmp.equals(val)))
                        list.add(index);
                    index++;
                }
            }
        }

        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    public T[] getValues(int start, int end) throws IOException {
        LinkedList<T> retval = new LinkedList<>();
        int count = 0;
        boolean first = true;
        for (Object bloc : values) {
            Object tmp = valuesCompressor.uncompress(bloc);
            int length = Array.getLength(tmp);
            if (start >= count + length) {
                count += length;
                continue;
            }
            if (end <= count) {
                break;
            }

            for (int i = first ? start - count : 0; i < length && i <= end; i++) {
                retval.add(type.cast(Array.get(tmp, i)));
                count++;
            }

            first = false;
        }
        return (T[]) retval.toArray();
    }

    public T[] getValues(int bloc) throws IOException {
        if (bloc < values.size() && bloc > 0)
            return (T[]) valuesCompressor.uncompress(values.get(bloc));
        return null;
    }

    public void disableIndexing() {
        enableIndexing = false;
        rows = null;
    }

    public boolean indexingEnabled() {
        return enableIndexing;
    }

}
