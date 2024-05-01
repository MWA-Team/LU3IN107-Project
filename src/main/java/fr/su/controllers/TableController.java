package fr.su.controllers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.database.Database;
import fr.su.handlers.table.LocalTableHandler;
import fr.su.handlers.table.RemoteTableHandler;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import javax.print.attribute.standard.Media;
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
        localTableHandler.createTable(tableBody);
        remoteTableHandler.createTable(tableBody);
        return Response.status(200).entity(tableBody).type(MediaType.APPLICATION_JSON).build();
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
