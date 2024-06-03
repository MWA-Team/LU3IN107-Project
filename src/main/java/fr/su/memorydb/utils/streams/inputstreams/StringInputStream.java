package fr.su.memorydb.utils.streams.inputstreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

public class StringInputStream extends InputStream {

    private final ByteArrayInputStream byteArrayInputStream;
    private int ite = 0;
    private int count = 0;
    private String value = null;
    private boolean repetitions;

    public StringInputStream(byte[] byteArray) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        repetitions = true;
    }

    public StringInputStream(byte[] byteArray, boolean repetitions) {
        this.byteArrayInputStream = new ByteArrayInputStream(byteArray);
        this.repetitions = repetitions;
    }

    public String readString() throws IOException {
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
            tmp = new byte[4];
            if (byteArrayInputStream.read(tmp) != 4) {
                throw new IOException("Failed to read 4 bytes for an Integer (length)");
            }
            int len = ByteBuffer.wrap(tmp).getInt();
            tmp = new byte[len];
            if (byteArrayInputStream.read(tmp) != len) {
                throw new IOException("Failed to read " + len + " bytes for a String");
            }
            value = new String(tmp, StandardCharsets.UTF_8);
        }
        return value;
    }

    private String readNoRepetitions() throws IOException {
        int code = byteArrayInputStream.read();
        if (code == -1)
            throw new IOException("End of stream reached");
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
