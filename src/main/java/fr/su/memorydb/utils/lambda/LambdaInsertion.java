package fr.su.memorydb.utils.lambda;

import org.apache.parquet.example.data.Group;

@FunctionalInterface
public interface LambdaInsertion {

    Object call(Group g, String field, int index);

}