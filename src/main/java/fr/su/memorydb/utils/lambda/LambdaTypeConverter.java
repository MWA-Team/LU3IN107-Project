package fr.su.memorydb.utils.lambda;

@FunctionalInterface
public interface LambdaTypeConverter<T> {

    T call(String o);

}
