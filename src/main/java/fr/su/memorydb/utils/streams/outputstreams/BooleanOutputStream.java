package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class BooleanOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Boolean prev;
    private int nb;
    private boolean first;

    public BooleanOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
    }

    public void writeBoolean(Boolean value) {
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    byteArrayOutputStream.write(nb);
                    byteArrayOutputStream.write(prev ? 1 : 0);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(nb);
                        byteArrayOutputStream.write(prev ? 1 : 0);
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
            byteArrayOutputStream.write(prev ? 1 : 0);
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
