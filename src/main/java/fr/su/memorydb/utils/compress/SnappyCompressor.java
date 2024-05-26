package fr.su.memorydb.utils.compress;

import fr.su.memorydb.utils.lambda.LambdaCompress;
import fr.su.memorydb.utils.lambda.LambdaUncompress;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyCompressor implements Compressor {

    private LambdaCompress lambdaCompressValues;
    private LambdaUncompress lambdaUncompressValues;

    public SnappyCompressor(LambdaCompress lambdaCompressValues, LambdaUncompress lambdaUncompressValues) {
        this.lambdaCompressValues = lambdaCompressValues;
        this.lambdaUncompressValues = lambdaUncompressValues;
    }

    @Override
    public byte[] compress(Object data) throws IOException {
        byte[] tmp = lambdaCompressValues.call(data);
        return Snappy.compress(tmp);
    }

    @Override
    public Object uncompress(Object data) throws IOException {
        byte[] tmp = Snappy.uncompress((byte[]) data);
        return lambdaUncompressValues.call(tmp);
    }

}
