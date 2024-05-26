package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class IndexInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;

    public IndexInputStream(byte[] byteArray) throws IOException {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
    }

    public Integer readInteger() throws IOException {
        byte[] tmp = new byte[4];
        int read = byteArrayInputStream.read(tmp);
        if (read == -1)
            throw new IOException("End of stream reached");
        if (read != 4) {
            throw new IOException("Failed to read 4 bytes for an index");
        }
        return ByteBuffer.wrap(tmp).getInt();
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
