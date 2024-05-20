package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class LongOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Long prev;
    private int nb;
    private boolean first;

    public LongOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeLong(Long value) throws IOException {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    byteArrayOutputStream.write(nb);
                    byte[] longBytes = ByteBuffer.allocate(8).putLong(prev).array();
                    byteArrayOutputStream.write(longBytes);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byte[] longBytes = ByteBuffer.allocate(8).putLong(prev).array();
                        byteArrayOutputStream.write(longBytes);
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
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(nb);
            byte[] longBytes = ByteBuffer.allocate(8).putLong(prev).array();
            byteArrayOutputStream.write(longBytes);
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
