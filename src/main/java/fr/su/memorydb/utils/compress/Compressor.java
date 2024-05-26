package fr.su.memorydb.utils.compress;

import java.io.IOException;

public interface Compressor {
    Object compress(Object data) throws IOException;
    Object uncompress(Object data) throws IOException;
}

