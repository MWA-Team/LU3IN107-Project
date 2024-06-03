package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class DoubleInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private Double value = null;
    private boolean repetitions;

    public DoubleInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        repetitions = true;
    }

    public DoubleInputStream(byte[] byteArray, boolean repetitions) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        this.repetitions = repetitions;
    }

    public Double readDouble() throws IOException {
        if (!repetitions)
            return readNoRepetitions();
        if (ite < count) {
            ite++;
        } else {
            byte[] tmp = new byte[4];
            int code;
            code = byteArrayInputStream.read(tmp);
            if (code == -1)
                throw new IOException("End of stream reached");
            if (code != 4) {
                throw new IOException("Failed to read 4 bytes for an Integer (count)");
            }
            count = ByteBuffer.wrap(tmp).getInt();
            ite = 1;
            if (count == 0) {
                value = null;
                return null;
            }
            tmp = new byte[8];
            if (byteArrayInputStream.read(tmp) != 8) {
                throw new IOException("Failed to read 8 bytes for a Double");
            }
            value = ByteBuffer.wrap(tmp).getDouble();
        }
        return value;
    }

    private Double readNoRepetitions() throws IOException {
        int code = byteArrayInputStream.read();
        if (code == -1)
            throw new IOException("End of stream reached");
        byte[] tmp = new byte[8];
        if (byteArrayInputStream.read(tmp) != 8) {
            throw new IOException("Failed to read 8 bytes for a Double");
        }
        return ByteBuffer.wrap(tmp).getDouble();
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
