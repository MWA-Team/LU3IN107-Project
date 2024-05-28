package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

public class StringInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;

    public StringInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
    }

    public String readString() throws IOException {
        int code = byteArrayInputStream.read();
        if (code == -1)
            throw new IOException("End of stream reached");
        if (code == 0)
            return null;
        byte[] tmp = new byte[4];
        if (byteArrayInputStream.read(tmp) != 4) {
            throw new IOException("Failed to read 4 bytes for an Integer (length)");
        }
        int len = ByteBuffer.wrap(tmp).getInt();
        tmp = new byte[len];
        if (byteArrayInputStream.read(tmp) != len) {
            throw new IOException("Failed to read " + len + " bytes for a String");
        }
        return new String(tmp, StandardCharsets.UTF_8);
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
