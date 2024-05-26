package fr.su.memorydb.controllers;

import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.insertion.LocalInsertionHandler;
import fr.su.memorydb.handlers.insertion.RemoteInsertionHandler;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;

@Path("/insert")
public class TableInsertion {

    @Inject
    LocalInsertionHandler localInsertionHandler;

    @Inject
    RemoteInsertionHandler remoteInsertionHandler;

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response insertion(File file) {
        try {
            int responseCode = 200;
            localInsertionHandler.insert(file);
            remoteInsertionHandler.insert(file);
            if (responseCode == 200) {
                return Response.status(200).entity("Insertion successful !\nIt now has " + Database.getInstance().getTables().get("test").rowsCounter + " rows !").type(MediaType.TEXT_PLAIN).build();
            } else {
                return Response.status(500).entity("Insertion failed !").type(MediaType.TEXT_PLAIN).build();
            }
        } catch (WrongTableFormatException e) {
            e.printStackTrace();
            return Response.status(500).entity("Error: " + e.getMessage()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
