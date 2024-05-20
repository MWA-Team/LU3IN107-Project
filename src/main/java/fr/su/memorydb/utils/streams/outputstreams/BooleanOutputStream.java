package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class BooleanOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Boolean prev;
    private int count;
    private boolean first;

    public BooleanOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        count = 1;
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
                    byteArrayOutputStream.write(count);
                    byteArrayOutputStream.write(prev ? 1 : 0);
                    prev = null;
                    count = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(0);
                else {
                    if (!prev.equals(value)) {
                        byteArrayOutputStream.write(count);
                        byteArrayOutputStream.write(prev ? 1 : 0);
                        count = 1;
                        prev = value;
                    } else
                        count++;
                }
            }
        }
    }

    public void finish() {
        if (prev == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(count);
            byteArrayOutputStream.write(prev ? 1 : 0);
            prev = null;
            count = 1;
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
