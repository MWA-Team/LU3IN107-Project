package fr.su.memorydb.database;

import com.aayushatharva.brotli4j.encoder.Encoder;
import fr.su.memorydb.utils.lambda.LambdaCompressValues;
import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import fr.su.memorydb.utils.lambda.LambdaUncompressValues;
import fr.su.memorydb.utils.streams.inputstreams.*;
import fr.su.memorydb.utils.streams.outputstreams.*;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.*;

public class Column<T> {

    private final Table table;
    private final String name;
    private final boolean stored;
    private final List<HashMap<T, byte[]>> rows;
    private List<byte[]> values;

    private Class<T> type;
    private LambdaInsertion lambda;
    private LambdaTypeConverter<T> converter;
    private LambdaCompressValues lambdaCompressValues;
    private LambdaUncompressValues lambdaUncompressValues;

    public Column() {
        name = "Injected";
        stored = true;
        rows = new ArrayList<>();
        converter = (String o) -> null;
        table = null;
        values = new ArrayList<>();
    }

    public Column(Table table, String name, boolean stored, Class<T> type) {
        this.table = table;
        this.name = name;
        this.stored = stored;
        this.rows = new ArrayList<>();
        this.type = type;
        this.values = new ArrayList<>();

        // Type management
        if (type.equals(Boolean.class)) {
            lambda = GroupValueSource::getBoolean;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Boolean.parseBoolean(o)) : null;
            lambdaCompressValues = (Object[] array) -> {
                BooleanOutputStream out = new BooleanOutputStream();
                for (int i = 0; i < array.length; i++) {
                    out.writeBoolean((Boolean) array[i]);
                }
                out.finish();
                byte[] bytes = out.toByteArray();
                out.close();
                return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                BooleanInputStream in = new BooleanInputStream(tmp);
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
        } else if (type.equals(Integer.class)) {
            lambda = GroupValueSource::getInteger;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Integer.parseInt(o)) : null;
            lambdaCompressValues = (Object[] array) -> {
                IntegerOutputStream out = new IntegerOutputStream();
                for (int i = 0; i < array.length; i++) {
                        out.writeInteger((Integer) array[i]);
                }
                out.finish();
                byte[] bytes = out.toByteArray();
                out.close();
                return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                IntegerInputStream in = new IntegerInputStream(tmp);
                LinkedList<Integer> uncompressedList = new LinkedList<>();
                int i = 0;
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
        } else if (type.equals(Long.class)) {
            lambda = GroupValueSource::getLong;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Long.parseLong(o)) : null;
            lambdaCompressValues = (Object[] array) -> {
                LongOutputStream out = new LongOutputStream();
                for (int i = 0; i < array.length; i++) {
                    out.writeLong((Long) array[i]);
                }
                out.finish();
                byte[] bytes = out.toByteArray();
                out.close();
                return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                LongInputStream in = new LongInputStream(tmp);
                LinkedList<Long> uncompressedList = new LinkedList<>();
                int i = 0;
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
        }  else if (type.equals(Float.class)) {
            lambda = GroupValueSource::getFloat;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Float.parseFloat(o)) : null;
            lambdaCompressValues = (Object[] array) -> {
                FloatOutputStream out = new FloatOutputStream();
                for (int i = 0; i < array.length; i++) {
                    out.writeFloat((Float) array[i]);
                }
                out.finish();
                byte[] bytes = out.toByteArray();
                out.close();
                return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                FloatInputStream in = new FloatInputStream(tmp);
                LinkedList<Float> uncompressedList = new LinkedList<>();
                int i = 0;
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
        } else if (type.equals(Double.class)) {
            lambda = GroupValueSource::getDouble;
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(Double.parseDouble(o)) : null;
            lambdaCompressValues = (Object[] array) -> {
               DoubleOutputStream out = new DoubleOutputStream();
               for (int i = 0; i < array.length; i++) {
                   out.writeDouble((Double) array[i]);
               }
               out.finish();
               byte[] bytes = out.toByteArray();
               out.close();
               return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                DoubleInputStream in = new DoubleInputStream(tmp);
                LinkedList<Double> uncompressedList = new LinkedList<>();
                int i = 0;
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
        } else {
            lambda = (Group g, String field, int index) -> {
                int i = g.getType().getFieldIndex(field);
                return g.getValueToString(i, index);
            };
            converter = (String o) -> !Objects.equals(o, "null") ? type.cast(o) : null;
            lambdaCompressValues = (Object[] array) -> {
                StringOutputStream out = new StringOutputStream();
                for (int i = 0; i < array.length; i++) {
                    out.writeString((String) array[i]);
                }
                out.finish();
                byte[] bytes = out.toByteArray();
                out.close();
                return Snappy.compress(bytes);
            };
            lambdaUncompressValues = (byte[] array) -> {
                byte[] tmp = Snappy.uncompress(array);
                StringInputStream in = new StringInputStream(tmp);
                LinkedList<Object> uncompressedList = new LinkedList<>();
                int i = 0;
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

    public String getName() {
        return name;
    }

    public int[] get(T val) throws IOException {
        LinkedList<Integer> list = new LinkedList<>();

        for (int i = 0; i < rows.size(); i++) {
            HashMap<T, byte[]> bloc = rows.get(i);
            byte[] indexes = bloc.get(val);
            if (indexes == null)
                continue;

            // Decompressing indexes
            byte[] tmp = Snappy.uncompress(indexes);
            IntegerInputStream in = new IntegerInputStream(tmp);
            while (true) {
                try {
                    int index = in.readInteger();
                    list.add(index);
                } catch (IOException e) {
                    break;
                }
            }
            in.close();
        }

        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    public void addRows(Object[] newRows) throws IOException {
        HashMap<T, LinkedList<Integer>> rows = new HashMap<>();

        for (int i = 0; i < newRows.length; i++) {
            LinkedList<Integer> indexes;
            boolean created = false;

            // If there was no index for this value, we create a list for that
            if (rows.containsKey(type.cast(newRows[i])))
                indexes = rows.get(type.cast(newRows[i]));
            else {
                indexes = new LinkedList<>();
                rows.put(type.cast(newRows[i]), indexes);
                created = true;
            }

            // Linking current index and this value
            indexes.add(table.rowsCounter - newRows.length + i);
        }

        // Adding all changes to the database
        List<Thread> threads = new ArrayList<>();
        HashMap<T, byte[]> newBloc = new HashMap<>();
        for (Map.Entry<T, LinkedList<Integer>> entry : rows.entrySet()) {
            int[] array = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
            Arrays.parallelSort(array);

            // Compressing indexes
            IntegerOutputStream out = new IntegerOutputStream();
            for (int i = 0; i < array.length; i++) {
                out.writeInteger(array[i]);
            }
            out.finish();
            byte[] bytes = out.toByteArray();
            out.close();
            byte[] compressedIndexes = Snappy.compress(bytes);

            // Adding values
            newBloc.put(entry.getKey(), compressedIndexes);
        }

        // Adding changes
        this.rows.add(newBloc);

        // Adding compressed values to the values
        values.add(lambdaCompressValues.call(newRows));
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

    public T get(int index) throws IOException {
        int count = 0;
        for (byte[] bloc : values) {
            Object[] tmp = lambdaUncompressValues.call(bloc);
            if (index <= tmp.length + count) {
                Object retval = tmp[index - count];
                return retval == null ? null : type.cast(retval);
            } else {
                count += tmp.length;
            }
        }
        throw new IndexOutOfBoundsException();
    }

    public T[] getValues(int start, int end) throws IOException {
        LinkedList<T> retval = new LinkedList<>();
        int count = 0;
        boolean first = true;
        for (byte[] bloc : values) {
            Object[] tmp = lambdaUncompressValues.call(bloc);
            if (start >= count + tmp.length) {
                count += tmp.length;
                continue;
            }
            if (end <= count) {
                break;
            }

            for (int i = first ? start - count : 0; i < tmp.length && i <= end; i++) {
                retval.add(type.cast(tmp[i]));
            }

            first = false;
        }
        return (T[]) retval.toArray();
    }

}
