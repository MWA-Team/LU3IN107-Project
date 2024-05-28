package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.table.LocalTableHandler;
import fr.su.memorydb.handlers.table.RemoteTableHandler;
import fr.su.memorydb.handlers.table.response.TableResponse;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.DetailsResponse;
import fr.su.memorydb.utils.response.ErrorResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

@Path("table")
public class TableController {

    @Inject
    LocalTableHandler localTableHandler;

    @Inject
    RemoteTableHandler remoteTableHandler;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response table(TableBody tableBody, @QueryParam("Server-Signature") String serverSignature) throws IOException, InterruptedException {
        Instant start = Instant.now();

        if(Database.getInstance().getTables().containsKey(tableBody.tableName)) {
            return Response.status(401).entity(new ErrorResponse(tableBody.tableName, "Table with this name already exist!")).build();
        }

        Thread thread = new Thread(() -> {
            try {
                remoteTableHandler.createTable(tableBody);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        localTableHandler.createTable(tableBody);
        thread.join();

        DetailsResponse response;
        if (serverSignature != null) {
            TableResponse tmp = new TableResponse(tableBody.tableName);
            for (Column column : Database.getInstance().getTables().get(tableBody.getTableName()).getColumns()) {
                if (column.stored())
                    tmp.addColumn(column);
            }
            response = tmp;
        } else
            response = new DetailsResponse(tableBody.tableName);

        response.setStart(start);

        return Response.status(200).entity(response.details("Table '" + tableBody.tableName + "' has been created!").done()).type(MediaType.APPLICATION_JSON).build();
    }

    public static class TableBody {

        @JsonProperty
        private String tableName;

        @JsonProperty
        private List<TableParameter> columns;

        public String getTableName() { return tableName; }

        public List<TableParameter> getColumns() { return columns; }

    }

    public static class TableParameter {

        private String name;
        private String type;

        public String getName() { return this.name; }

        public String getType() { return this.type; }
    }

}   
