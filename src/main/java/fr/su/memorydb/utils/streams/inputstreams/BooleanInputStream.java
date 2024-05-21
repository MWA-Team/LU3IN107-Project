package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BooleanInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private Boolean value = null;

    public BooleanInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
    }

    public Boolean readBoolean() throws IOException {
        if (ite < count) {
            ite++;
        } else {
            byte[] tmp = new byte[4];
            if (byteArrayInputStream.read(tmp) != 4) {
                throw new IOException("Failed to read 4 bytes for an Integer (count)");
            }
            count = ByteBuffer.wrap(tmp).getInt();
            ite = 1;
            if (count == -1) {
                throw new IOException("End of stream reached");
            }
            if (count == 0) {
                value = null;
                return null;
            }
            int bool = byteArrayInputStream.read();
            if (bool == -1) {
                throw new IOException("Failed to read 4 bytes for a Boolean");
            }
            value = bool == 1;
        }
        return value;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}