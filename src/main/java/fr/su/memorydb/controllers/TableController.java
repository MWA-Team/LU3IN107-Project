package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.table.LocalTableHandler;
import fr.su.memorydb.handlers.table.RemoteTableHandler;
import fr.su.memorydb.utils.response.DetailsResponse;
import fr.su.memorydb.utils.response.ErrorResponse;
import fr.su.memorydb.utils.response.ResponseTime;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
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
    public Response table(TableBody tableBody) throws IOException {

        DetailsResponse detailsResponse = new DetailsResponse(tableBody.tableName);

        if(Database.getInstance().getTables().containsKey(tableBody.tableName)) {
            return Response.status(401).entity(new ErrorResponse(tableBody.tableName, "Table with this name already exist!")).build();
        }

        localTableHandler.createTable(tableBody);
        remoteTableHandler.createTable(tableBody);
        return Response.status(200).entity(detailsResponse.details("Table '" + tableBody.tableName + "' has been created!").done()).type(MediaType.APPLICATION_JSON).build();
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
