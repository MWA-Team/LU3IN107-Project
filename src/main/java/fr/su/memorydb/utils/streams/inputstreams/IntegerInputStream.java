package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class IntegerInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private Integer value = null;

    public IntegerInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
    }

    public Integer readInteger() throws IOException {
        if (ite < count) {
            ite++;
        } else {
            count = byteArrayInputStream.read();
            ite = 1;
            if (count == -1) {
                throw new IOException("End of stream reached");
            }
            if (count == 0) {
                value = null;
                return null;
            }
            byte[] tmp = new byte[4];
            if (byteArrayInputStream.read(tmp) != 4) {
                throw new IOException("Failed to read 4 bytes for an Integer");
            }
            value = ByteBuffer.wrap(tmp).getInt();
        }
        return value;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
