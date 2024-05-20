package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DoubleOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Double prev;
    private int nb;
    private boolean first;

    public DoubleOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeDouble(Double value) throws IOException {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    byteArrayOutputStream.write(nb);
                    byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(prev).array();
                    byteArrayOutputStream.write(doubleBytes);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(prev).array();
                        byteArrayOutputStream.write(doubleBytes);
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
            byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(prev).array();
            byteArrayOutputStream.write(doubleBytes);
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
