package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class BooleanInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private Boolean value = null;
    private boolean repetitions;

    public BooleanInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        repetitions = true;
    }

    public BooleanInputStream(byte[] byteArray, boolean repetitions) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        this.repetitions = repetitions;
    }

    public Boolean readBoolean() throws IOException {
        if (!repetitions)
            return readNoRepetitions();
        if (ite < count) {
            ite++;
        } else {
            byte[] tmp = new byte[4];
            int code;
            code = byteArrayInputStream.read(tmp);
            if (code == -1) {
                throw new IOException("End of stream reached");
            }
            if (code != 4) {
                throw new IOException("Failed to read 4 bytes for an Integer (count)");
            }
            count = ByteBuffer.wrap(tmp).getInt();
            ite = 1;
            if (count == 0) {
                value = null;
                return null;
            }
            int bool = byteArrayInputStream.read();
            if (bool == -1) {
                throw new IOException("Failed to read 1 byte for a Boolean");
            }
            value = bool == 1;
        }
        return value;
    }

    private Boolean readNoRepetitions() throws IOException {
        int code = byteArrayInputStream.read();
        if (code == -1)
            throw new IOException("End of stream reached");
        int bool = byteArrayInputStream.read();
        if (bool == -1) {
            throw new IOException("Failed to read 1 byte for a Boolean");
        }
        return bool == 1;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
