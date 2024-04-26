package fr.su.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;
import fr.su.handlers.select.LocalSelectHandler;
import fr.su.handlers.select.RemoteSelectHandler;
import fr.su.handlers.select.response.SelectResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.ws.rs.core.Response;

import javax.print.attribute.standard.Media;
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

        List<SelectResponse> list = new ArrayList<>();
        list.add(remoteResponse);
        SelectResponse finaleResponse = localResponse.merge(list, selectBody);

        return Response.status(200).entity(finaleResponse).type(MediaType.APPLICATION_JSON).build();
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

    public enum Operand { EQUALS, BIGGER, LOWER, NOT_EQUALS}
}