package fr.su.memorydb.controllers;

import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.insertion.LocalInsertionHandler;
import fr.su.memorydb.handlers.insertion.RemoteInsertionHandler;
import fr.su.memorydb.handlers.insertion.response.InsertResponse;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Path("/insert")
public class TableInsertion {

    @Inject
    LocalInsertionHandler localInsertionHandler;

    @Inject
    RemoteInsertionHandler remoteInsertionHandler;

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response insertion(@QueryParam("table") String tableName, File file) {
        Instant start = Instant.now();
        Table table = Database.getInstance().getTables().get(tableName);
        InsertResponse response = new InsertResponse(table.getName());
        try {
            int responseCode = 200;
            localInsertionHandler.insert(file, tableName);
            remoteInsertionHandler.insert(file, tableName);
            if (responseCode == 200) {
                response.setSeconds(Duration.between(start, Instant.now()));
                response.setRows(table.rowsCounter);
                return Response.status(200).entity(response).type(MediaType.APPLICATION_JSON).build();
            } else {
                return Response.status(500).type(MediaType.APPLICATION_JSON).build();
            }
        } catch (WrongTableFormatException e) {
            e.printStackTrace();
            return Response.status(500).entity("Error: " + e.getMessage()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
