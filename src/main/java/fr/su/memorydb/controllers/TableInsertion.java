package fr.su.memorydb.controllers;

import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.insertion.LocalInsertionHandler;
import fr.su.memorydb.handlers.insertion.RemoteInsertionHandler;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.InsertResponse;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
import fr.su.memorydb.utils.response.ErrorResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

@Path("/insert")
public class TableInsertion {

    @Inject
    LocalInsertionHandler localInsertionHandler;

    @Inject
    RemoteInsertionHandler remoteInsertionHandler;

    @Context
    RoutingContext routingContext;

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response insertion(@QueryParam("table") String tableName, File file) {
        Instant start = Instant.now();

        Table table = Database.getInstance().getTables().get(tableName);

        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(tableName, "Table '" + tableName + "' not found.").setStart(start).done()).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        InsertResponse response = new InsertResponse(tableName);
        try {
            Thread thread = new Thread(() -> {
                try {
                    remoteInsertionHandler.insert(file, tableName);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            localInsertionHandler.insert(file, tableName);
            thread.join();
            response.setRows(table.rowsCounter);
            return Response.status(200).entity(response.details("Insertion successful !").setStart(start).done()).type(MediaType.APPLICATION_JSON).build();
        } catch (WrongTableFormatException e) {
            e.printStackTrace();
            return Response.status(500).entity(new ErrorResponse(tableName, e.getMessage()).setStart(start).done()).build();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
