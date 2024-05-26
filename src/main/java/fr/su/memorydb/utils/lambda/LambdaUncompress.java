package fr.su.memorydb.utils.lambda;

import java.io.IOException;

@FunctionalInterface
public interface LambdaUncompress {

    Object call(byte[] data) throws IOException;

}
