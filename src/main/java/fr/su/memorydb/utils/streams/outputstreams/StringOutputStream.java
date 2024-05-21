package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private String prev;
    private int nb;
    private boolean first;

    public StringOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeString(String value) throws IOException {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
                else {
                    byteArrayOutputStream.write(nb);
                    byte[] stringBytes = prev.getBytes(StandardCharsets.UTF_8);
                    byteArrayOutputStream.write(stringBytes.length);
                    byteArrayOutputStream.write(stringBytes);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byte[] stringBytes = prev.getBytes(StandardCharsets.UTF_8);
                        byteArrayOutputStream.write(stringBytes.length);
                        byteArrayOutputStream.write(stringBytes);
                        nb = 1;
                        prev = value;
                    } else
                        nb++;
                }
            }
        }
    }

    public void finish() throws IOException {
        if (prev == null)
            byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
        else {
            byteArrayOutputStream.write(nb);
            byte[] stringBytes = prev.getBytes(StandardCharsets.UTF_8);
            byteArrayOutputStream.write(stringBytes.length);
            byteArrayOutputStream.write(stringBytes);
            prev = null;
            nb = 1;
        }
        first = true;
    }

    @Override
    public void write(int b) {
        byteArrayOutputStream.write(b);
    }

    public byte[] toByteArray() {
        return byteArrayOutputStream.toByteArray();
    }

}
