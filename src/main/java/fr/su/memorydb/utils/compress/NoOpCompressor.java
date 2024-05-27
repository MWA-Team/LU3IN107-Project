package fr.su.memorydb.utils.compress;

import java.lang.reflect.Array;

public class NoOpCompressor implements Compressor{

    @Override
    public Object compress(Object data, int size) {
        if (Array.getLength(data) > size) {
            Object[] retval = new Object[size];
            for (int i = 0; i < size; i++) {
                retval[i] = Array.get(data, i);
            }
            return retval;
        }
        return data;
    }

    @Override
    public Object uncompress(Object data) {
        return data;
    }

}
