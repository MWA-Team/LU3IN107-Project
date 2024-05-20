package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private String value = null;

    public StringInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
    }

    public String readString() throws IOException {
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
            int len = byteArrayInputStream.read();
            byte[] tmp = new byte[len];
            if (byteArrayInputStream.read(tmp) != len) {
                throw new IOException("Failed to read " + len + " bytes for a String");
            }
            value = new String(tmp, StandardCharsets.UTF_8);
        }
        return value;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Use the other methods to parse the stream instead.");
    }

}
