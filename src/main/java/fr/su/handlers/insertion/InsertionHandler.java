package fr.su.handlers.insertion;

import fr.su.utils.exceptions.TableColumnSizeException;
import fr.su.utils.exceptions.WrongTableFormatException;

import java.io.IOException;
import java.io.InputStream;

public interface InsertionHandler {

    public int insert(InputStream inputStream) throws IOException, WrongTableFormatException, TableColumnSizeException; //Return status code for validation
}
