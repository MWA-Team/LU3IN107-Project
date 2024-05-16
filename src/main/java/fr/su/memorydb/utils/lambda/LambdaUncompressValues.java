package fr.su.memorydb.utils.lambda;

import java.io.IOException;

public interface LambdaUncompressValues {

    Object[] call(byte[] data) throws IOException;

}
