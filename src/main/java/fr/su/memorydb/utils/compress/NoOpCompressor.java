package fr.su.memorydb.utils.compress;

import java.io.IOException;

public class NoOpCompressor implements Compressor{
    @Override
    public byte[] compress(byte[] data) throws IOException {
        return data;
    }

    @Override
    public byte[] uncompress(byte[] data) throws IOException {
        return data;
    }
}
