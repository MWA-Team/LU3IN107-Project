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

    @Inject
    RoutingContext context;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response table(TableBody tableBody) throws IOException, InterruptedException {
        Instant start = Instant.now();

        String server_id = context.queryParams().get("server_id");
        Thread thread = new Thread(() -> {
            localTableHandler.createTable(tableBody, server_id);
        });
        thread.start();
        remoteTableHandler.createTable(tableBody, null);
        thread.join();

        DetailsResponse response;
        if (context.queryParams().get("Server-Signature") != null) {
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
