package fr.su.memorydb.database;

import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import com.aayushatharva.brotli4j.decoder.DirectDecompress;
import com.aayushatharva.brotli4j.encoder.Encoder;
import fr.su.memorydb.utils.lambda.LambdaCompressValues;
import fr.su.memorydb.utils.lambda.LambdaInsertion;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import fr.su.memorydb.utils.lambda.LambdaUncompressValues;
import fr.su.memorydb.utils.streams.inputstreams.*;
import fr.su.memorydb.utils.streams.outputstreams.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupValueSource;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class Column<T> {

    private final Table table;
    private final String name;
    private final boolean stored;
    private final HashMap<T, byte[]> rows;
    private byte[] values = null;

    // Change this value to influence the level of compression
    private Encoder.Parameters compressParams = new Encoder.Parameters().setQuality(1);

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
            lambdaCompressValues = (Object[] array) -> {
                BooleanOutputStream out = new BooleanOutputStream();

                for (int i = 0; i < array.length; i++) {
                    out.writeBoolean((Boolean) array[i]);
                }
                out.finish();
                return Encoder.compress(out.toByteArray(), compressParams);
            };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                BooleanInputStream in = new BooleanInputStream(tmp.getDecompressedData());
                Boolean[] uncompressedArray = new Boolean[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readBoolean();
                }
                return uncompressedArray;
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
                return Encoder.compress(out.toByteArray(), compressParams);
            };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                IntegerInputStream in = new IntegerInputStream(tmp.getDecompressedData());
                Integer[] uncompressedArray = new Integer[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readInteger();
                }
                return uncompressedArray;
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
                return Encoder.compress(out.toByteArray(), compressParams);
            };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                LongInputStream in = new LongInputStream(tmp.getDecompressedData());
                Long[] uncompressedArray = new Long[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readLong();
                }
                return uncompressedArray;
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
                return Encoder.compress(out.toByteArray(), compressParams);
            };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                FloatInputStream in = new FloatInputStream(tmp.getDecompressedData());
                Float[] uncompressedArray = new Float[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readFloat();
                }
                return uncompressedArray;
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
                return Encoder.compress(out.toByteArray(), compressParams);
           };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                DoubleInputStream in = new DoubleInputStream(tmp.getDecompressedData());
                Double[] uncompressedArray = new Double[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readDouble();
                }
                return uncompressedArray;
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
                return Encoder.compress(out.toByteArray(), compressParams);
            };
            lambdaUncompressValues = (byte[] array) -> {
                DirectDecompress tmp = Decoder.decompress(array);
                if (tmp.getResultStatus() != DecoderJNI.Status.DONE)
                    throw new IOException("Decompression failed");
                StringInputStream in = new StringInputStream(tmp.getDecompressedData());
                String[] uncompressedArray = new String[table.getRowsCounter()];
                for (int i = 0; i < table.getRowsCounter(); i++) {
                    uncompressedArray[i] = in.readString();
                }
                return uncompressedArray;
            };
        }
    }

    public String getName() {
        return name;
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
        T[] tmp = (T[]) Array.newInstance(type, table.getRowsCounter());
        for (Map.Entry<T, byte[]> row : rows.entrySet()) {
            int[] indexes = Snappy.uncompressIntArray(row.getValue());
            for (int i = 0; i < indexes.length; i++) {
                T val = row.getKey();
                tmp[indexes[i]] = val;
            }
        }
        values = lambdaCompressValues.call(tmp);
        return;
    }

    public T[] getValues() throws IOException {
        return (T[]) lambdaUncompressValues.call(values);
    }
}
