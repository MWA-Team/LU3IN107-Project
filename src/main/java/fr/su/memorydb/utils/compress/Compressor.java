package fr.su.memorydb.utils.compress;

import java.io.IOException;

public interface Compressor {
    Object compress(Object data, int size) throws IOException;
    Object uncompress(Object data) throws IOException;
}

