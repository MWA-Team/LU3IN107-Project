package fr.su.memorydb.utils.lambda;

import java.io.IOException;

@FunctionalInterface
public interface LambdaCompressValues {

    byte[] call(Object[] values) throws IOException;

}
