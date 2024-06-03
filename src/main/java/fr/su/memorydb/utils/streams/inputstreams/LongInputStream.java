package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class LongInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private Long value = null;
    private boolean repetitions;

    public LongInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        repetitions = true;
    }

    public LongInputStream(byte[] byteArray, boolean repetitions) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        this.repetitions = repetitions;
    }

    public Long readLong() throws IOException {
        if (!repetitions)
            return readNoRepetitions();
        if (ite < count) {
            ite++;
        } else {
            byte[] tmp = new byte[4];
            int code = byteArrayInputStream.read(tmp);
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
                throw new IOException("Failed to read 8 bytes for a Long");
            }
            value = ByteBuffer.wrap(tmp).getLong();
        }
        return value;
    }

    private Long readNoRepetitions() throws IOException {
        int code = byteArrayInputStream.read();
        if (code == -1)
            throw new IOException("End of stream reached");
        byte[] tmp = new byte[8];
        if (byteArrayInputStream.read(tmp) != 8) {
            throw new IOException("Failed to read 8 bytes for a Long");
        }
        return ByteBuffer.wrap(tmp).getLong();
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
