package fr.su.utils.exceptions;

public class WrongTableFormatException extends Exception {
    public WrongTableFormatException() {
        super("Parquet Shema is not same as DataBase Shema");
    }
}
