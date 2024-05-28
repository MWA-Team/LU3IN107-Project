package fr.su.memorydb.utils.streams.outputstreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringOutputStream extends OutputStream {

    private ByteArrayOutputStream byteArrayOutputStream;

    public StringOutputStream() {
        byteArrayOutputStream = new ByteArrayOutputStream();
    }

    public void writeString(String value) throws IOException {
        if (value == null)
            byteArrayOutputStream.write(0);
        else {
            byteArrayOutputStream.write(1);
            byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
            byte[] lenBytes = ByteBuffer.allocate(4).putInt(stringBytes.length).array();
            byteArrayOutputStream.write(lenBytes);
            byteArrayOutputStream.write(stringBytes);
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
