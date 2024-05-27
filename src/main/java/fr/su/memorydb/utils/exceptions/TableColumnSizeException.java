package fr.su.memorydb.utils.exceptions;

public class TableColumnSizeException extends Exception {

    public TableColumnSizeException(int dbNumber, int fileNumber) {
        super("Number of columns is not the same between our database (" + dbNumber + ") and your file (" + fileNumber + ")");
    }

}
