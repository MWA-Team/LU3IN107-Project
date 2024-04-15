package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;

@Path("/insert")
public class TableInsertion {

    @Inject
    ForwardingManager forwardingManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getFile(String body) throws IOException {

        /**
         *
         * EXEMPLE
         *
         * ApacheReaderFile f;
         *
         * List<String> strct = f.getStructure(); //Toutes les colonnes
         * HashMap<String, List<String>> content;
         *
         * for(String column : strct)
         *
         *      content.put(column, new ArrayList<>()); //création d'une colonne vide
         *
         *      content.get(column).addAll(f.getColumn(column));
         *
         *
         */
        Response response = Response.status(200).entity("This is fine").build();
        Response forward = forwardingManager.forwardPost(body);
        // If forward is null, the query was not forwarded, else check the status code and deal with it
        return forward == null ? response : forward;
    }

}
