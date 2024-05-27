package fr.su.memorydb.handlers.insertion;

import fr.su.memorydb.utils.exceptions.TableColumnSizeException;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;

import java.io.File;
import java.io.IOException;

public interface InsertionHandler {

    public int insert(File file) throws IOException, WrongTableFormatException, TableColumnSizeException;

}
