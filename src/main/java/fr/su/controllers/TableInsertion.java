package fr.su.controllers;

import fr.su.handlers.insertion.LocalInsertionHandler;
import fr.su.handlers.insertion.RemoteInsertionHandler;
import fr.su.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.util.Map;

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
                return Response.ok().build();
            } else {
                return Response.status(500).build();
            }
        } catch (WrongTableFormatException e) {
            e.printStackTrace();
            return Response.status(500).entity("Error: " + e.getMessage()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

   /* Map<String, Map<Integer, Object>> parquetData = localInsertionHandler.parseParquet();
                parquetData.forEach((column, values) -> {
                        System.out.println("Column: " + column);
                        values.forEach((row, value) -> {
                        System.out.println("Row " + row + ": " + value);
                        });
                        });

    */
