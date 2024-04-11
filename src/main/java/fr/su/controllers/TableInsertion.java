package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;

@Path("/insert")
public class TableInsertion {

    @Inject
    ForwardingManager forwardingManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Object getFile(String body) throws IOException {

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

        return forwardingManager.forwardPost(body);
    }

}
