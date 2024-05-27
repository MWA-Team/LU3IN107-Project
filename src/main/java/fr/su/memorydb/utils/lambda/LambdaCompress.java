package fr.su.memorydb.utils.lambda;

import java.io.IOException;

@FunctionalInterface
public interface LambdaCompress {

    byte[] call(Object values, int size) throws IOException;

}