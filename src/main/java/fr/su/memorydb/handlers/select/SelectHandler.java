package fr.su.memorydb.handlers.select;

import com.fasterxml.jackson.core.JsonProcessingException;
import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.handlers.select.response.SelectResponse;

import java.io.IOException;

public interface SelectHandler {

    /**
     *
     * @param query JSON Body which contains the query
     * @return status code (if local or remote has responded successfully)
     */
    public SelectResponse select(TableSelection.SelectBody selectBody) throws IOException, JsonProcessingException;
}
