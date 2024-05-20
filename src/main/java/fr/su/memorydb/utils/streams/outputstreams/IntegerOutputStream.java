package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class IntegerOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Integer prev;
    private int nb;
    private boolean first;

    public IntegerOutputStream() {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeInteger(Integer value) {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    byteArrayOutputStream.write(nb);
                    byteArrayOutputStream.write((prev >>> 24) & 0xFF);
                    byteArrayOutputStream.write((prev >>> 16) & 0xFF);
                    byteArrayOutputStream.write((prev >>> 8) & 0xFF);
                    byteArrayOutputStream.write(prev & 0xFF);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byteArrayOutputStream.write((prev >>> 24) & 0xFF);
                        byteArrayOutputStream.write((prev >>> 16) & 0xFF);
                        byteArrayOutputStream.write((prev >>> 8) & 0xFF);
                        byteArrayOutputStream.write(prev & 0xFF);
                        nb = 1;
                        prev = value;
                    } else
                        nb++;
                }
            }
        }
    }

    public void finish() {
        if (prev == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(nb);
            byteArrayOutputStream.write((prev >>> 24) & 0xFF);
            byteArrayOutputStream.write((prev >>> 16) & 0xFF);
            byteArrayOutputStream.write((prev >>> 8) & 0xFF);
            byteArrayOutputStream.write(prev & 0xFF);
            prev = null;
            nb = 1;
        }
        first = true;
    }

    @Override
    public void write(int b) throws IOException {
        byteArrayOutputStream.write(b);
    }

    public byte[] toByteArray() {
        return byteArrayOutputStream.toByteArray();
    }

}
