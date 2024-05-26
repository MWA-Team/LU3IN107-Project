package fr.su.memorydb.utils.compress;

import java.io.IOException;

public interface Compressor {
    byte[] compress(byte[] data) throws IOException;
    byte[] uncompress(byte[] data) throws IOException;
}

