package fr.su.handlers.insertion;

import org.apache.parquet.example.data.Group;

@FunctionalInterface
public interface LambdaInsertion {

    Object call(Group g, int field, int index);

}
