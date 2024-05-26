package fr.su.memorydb.utils.compress;

import java.io.IOException;

public class NoOpCompressor implements Compressor{

    @Override
    public Object compress(Object data) {
        return data;
    }

    @Override
    public Object uncompress(Object data) {
        return data;
    }

}
