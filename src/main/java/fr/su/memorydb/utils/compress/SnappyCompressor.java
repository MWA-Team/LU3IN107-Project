package fr.su.memorydb.utils.compress;

import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyCompressor implements Compressor {
    @Override
    public byte[] compress(byte[] data) throws IOException {
        return Snappy.compress(data);
    }

    @Override
    public byte[] uncompress(byte[] data) throws IOException {
        return Snappy.uncompress(data);
    }
}
