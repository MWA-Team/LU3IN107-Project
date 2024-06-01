package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.utils.response.RowsResponse;
import fr.su.memorydb.utils.response.SelectResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public interface SelectHandler {

    /**
     * This method parses the possible "where" conditions of the request and gets all the valid indexes that this server
     * can get.
     * @param whereBody JSON Body which contains the "where" query
     * @return an array containing all valid indexes for the "where" condition if it exists or null if it doesn't.
     */
    int[] where(TableSelection.WhereBody whereBody) throws IOException, InterruptedException;

    /**
     * This method returns the response containing all the rows that need to be shown, filtered with the indexes given
     * by the array in parameters.
     * @param selectBody JSON Body which contains the query
     * @param indexes Array containing all the indexes we need to get
     * @return The final response containing all rows corresponding to the request
     */
    List<HashMap<String, Object>> select(TableSelection.SelectBody selectBody, int[] indexes) throws IOException, InterruptedException;

}
