package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IndexOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;

    public IndexOutputStream() {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
    }

    public void writeIndex(Integer value) throws IOException {
        if (value == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(1);
            byte[] intBytes = ByteBuffer.allocate(4).putInt(value).array();
            byteArrayOutputStream.write(intBytes);
        }
    }

    @Override
    public void write(int b) throws IOException {
        byteArrayOutputStream.write(b);
    }

    public byte[] toByteArray() {
        return byteArrayOutputStream.toByteArray();
    }

}
