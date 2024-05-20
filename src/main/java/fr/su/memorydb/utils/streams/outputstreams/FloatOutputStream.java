package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class FloatOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Float prev;
    private int nb;
    private boolean first;

    public FloatOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeFloat(Float value) throws IOException {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    byteArrayOutputStream.write(nb);
                    byte[] floatBytes = ByteBuffer.allocate(4).putFloat(prev).array();
                    byteArrayOutputStream.write(floatBytes);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byte[] floatBytes = ByteBuffer.allocate(4).putFloat(prev).array();
                        byteArrayOutputStream.write(floatBytes);
                        nb = 1;
                        prev = value;
                    } else
                        nb++;
                }
            }
        }

        if (value == null) {
            byteArrayOutputStream.write(0);
        } else {
            byteArrayOutputStream.write(1);
            byte[] floatBytes = ByteBuffer.allocate(4).putFloat(prev).array();
            byteArrayOutputStream.write(floatBytes);
        }
    }

    public void finish() throws IOException {
        if (prev == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(nb);
            byte[] floatBytes = ByteBuffer.allocate(4).putFloat(prev).array();
            byteArrayOutputStream.write(floatBytes);
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
