package fr.su.handlers.insertion;

import fr.su.utils.exceptions.TableColumnSizeException;
import fr.su.utils.exceptions.WrongTableFormatException;

import java.io.File;
import java.io.IOException;

public interface InsertionHandler {

    public int insert(File file) throws IOException, WrongTableFormatException, TableColumnSizeException;
}
