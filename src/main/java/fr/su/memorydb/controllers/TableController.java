package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.table.LocalTableHandler;
import fr.su.memorydb.handlers.table.RemoteTableHandler;
import fr.su.memorydb.utils.response.ErrorResponse;
import fr.su.memorydb.utils.response.TableResponse;
import fr.su.memorydb.utils.ToolBox;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("table")
public class TableController {

    @Inject
    LocalTableHandler localTableHandler;

    @Inject
    RemoteTableHandler remoteTableHandler;

    @Context
    RoutingContext routingContext;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response table(TableBody tableBody) throws IOException, InterruptedException {
        Instant start = Instant.now();

        if(Database.getInstance().getTables().containsKey(tableBody.tableName)) {
            return Response.status(401).entity(new ErrorResponse(tableBody.tableName, "Table with this name already exist!").setStart(start).done()).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);
        ToolBox.columnsRepartition.computeIfAbsent(tableBody.getTableName(), k -> new HashMap<>());

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

        TableResponse response = new TableResponse(tableBody.tableName);
        for (Map.Entry<String, Integer> entry : ToolBox.columnsRepartition.get(tableBody.getTableName()).entrySet()) {
            response.addColumn(entry.getKey());
        }

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
