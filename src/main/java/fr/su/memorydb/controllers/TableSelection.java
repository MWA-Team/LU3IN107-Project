package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.select.LocalSelectHandler;
import fr.su.memorydb.handlers.select.RemoteSelectHandler;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Path("select")
public class TableSelection{

    @Inject
    private LocalSelectHandler localSelectHandler;

    @Inject
    private RemoteSelectHandler remoteSelectHandler;

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectColumns(SelectBody selectBody) throws IOException {
        Database database = Database.getInstance();
        Table table = database.getTables().get(selectBody.table);
        if (table == null) {
            return Response.status(404).entity("Table '" + selectBody.table + "' not found.").build();
        }

        SelectResponse localResponse = localSelectHandler.select(selectBody);
        SelectResponse remoteResponse = remoteSelectHandler.select(selectBody);

        int statusCode = 200;

        if (localResponse == null && remoteResponse == null)
            statusCode = 204;
x   
        List<SelectResponse> list = new ArrayList<>();
        list.add(remoteResponse);
        SelectResponse finaleResponse = localResponse != null ? localResponse.merge(list) : remoteResponse != null ? remoteResponse : new SelectResponse();

        return Response.status(statusCode).entity(finaleResponse).type(MediaType.APPLICATION_JSON).build();
    }

    public static class SelectBody {

        @JsonProperty
        private String table;

        @JsonProperty
        List<String> columns;
        @JsonProperty
        HashMap<String, SelectOperand> where;

        public String getTable() {
            return table;
        }

        public List<String> getColumns() {
            return columns;
        }

        public HashMap<String, SelectOperand> getWhere() {
            return where;
        }
    }

    public static class SelectOperand {

        @JsonProperty
        private Operand operand;
        @JsonProperty
        private String value;

        public Operand getOperand() {
            return operand;
        }

        public String getValue() {
            return value;
        }
    }

    public enum Operand { EQUALS, BIGGER, LOWER, NOT_EQUALS }
}