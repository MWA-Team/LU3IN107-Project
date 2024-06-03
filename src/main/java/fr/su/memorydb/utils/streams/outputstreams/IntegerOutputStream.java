package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IntegerOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;
    private Integer prev;
    private int nb;
    private boolean first;
    private boolean repetitions;

    public IntegerOutputStream() {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
        repetitions = true;
    }

    public IntegerOutputStream(boolean repetitions) {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        prev = null;
        nb = 1;
        first = true;
        this.repetitions = repetitions;
    }

    public void writeInteger(Integer value) throws IOException {
        if (!repetitions) {
            writeNoRepetitions(value);
            return;
        }
        if (first) {
            first = false;
            prev = value;
        } else {
            if (value == null) {
                if (prev == null)
                    byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
                else {
                    byte[] nbBytes = ByteBuffer.allocate(4).putInt(nb).array();
                    byteArrayOutputStream.write(nbBytes);
                    byte[] intBytes = ByteBuffer.allocate(4).putInt(prev).array();
                    byteArrayOutputStream.write(intBytes);
                    prev = null;
                    nb = 1;
                }
            } else {
                if (prev == null)
                    byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
                else {
                    if (!prev.equals(value)) {
                        byte[] nbBytes = ByteBuffer.allocate(4).putInt(nb).array();
                        byteArrayOutputStream.write(nbBytes);
                        byte[] intBytes = ByteBuffer.allocate(4).putInt(prev).array();
                        byteArrayOutputStream.write(intBytes);
                        nb = 1;
                        prev = value;
                    } else
                        nb++;
                }
            }
        }
    }

    public void finish() throws IOException {
        if (!repetitions)
            return;
        if (prev == null)
            byteArrayOutputStream.write(new byte[]{0, 0, 0, 0});
        else {
            byte[] nbBytes = ByteBuffer.allocate(4).putInt(nb).array();
            byteArrayOutputStream.write(nbBytes);
            byte[] intBytes = ByteBuffer.allocate(4).putInt(prev).array();
            byteArrayOutputStream.write(intBytes);
            prev = null;
            nb = 1;
        }
        first = true;
    }

    private void writeNoRepetitions(Integer value) throws IOException {
        if (value == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(1);
            byte[] intBytes = ByteBuffer.allocate(4).putInt(value).array();
            byteArrayOutputStream.write(intBytes);
        }
    }

    @Override
    public void write(int b) {
        byteArrayOutputStream.write(b);
    }

    public byte[] toByteArray() {
        return byteArrayOutputStream.toByteArray();
    }

}
