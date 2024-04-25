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
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectColumns(SelectBody selectBody) throws IOException {
        Database database = Database.getInstance();
        Table table = database.getTables().get(selectBody.table);
        if (table == null) {
            System.out.println("Here is first");
            return Response.status(404).entity("Table '" + selectBody.table + "' not found.").build();
        }

        SelectResponse localResponse = localSelectHandler.select(selectBody);
        SelectResponse remoteResponse = remoteSelectHandler.select(selectBody);

        List<SelectResponse> list = new ArrayList<>();
        list.add(remoteResponse);
        SelectResponse finaleResponse = localResponse.merge(list);

        JsonObject resultObject = new JsonObject();
        JsonArray dataArray = new JsonArray();
        for (int i : finaleResponse.getIndexes()) {
            JsonObject rowObject = new JsonObject();
            for (Column column : finaleResponse.getColumns()) {

                if(!selectBody.getColumns().contains(column.getName())) continue;

                Object value = column.getValues().get(i);
                rowObject.addProperty(column.getName(), value == null ? "" : value.toString());
            }
            dataArray.add(rowObject);
        }
        resultObject.add("data", dataArray);
        System.out.println("Here is last");
        return Response.status(200).entity(resultObject.toString()).type(MediaType.APPLICATION_JSON).build();

        /*JsonObject jsonObject = JsonParser.parseString(jsonBody).getAsJsonObject();
        String tableName = jsonObject.get("table").getAsString();
        JsonArray columnsArray = jsonObject.getAsJsonArray("columns");
        List<String> columns = new ArrayList<>();
        for (JsonElement element : columnsArray) {
            columns.add(element.getAsString());
        }

        Table table = database.getTables().get(tableName);
        if (table == null) {
            return "Table '" + tableName + "' not found.";
        }

        JsonObject resultObject = new JsonObject();
        JsonArray dataArray = new JsonArray();
        for (int i = 0; i < table.getColumns().get(columns.get(0)).getValues().size(); i++) {
            JsonObject rowObject = new JsonObject();
            for (String column : columns) {
                if (table.getColumns().containsKey(column)) {
                    Object value = table.getColumns().get(column).getValues().get(i);
                    rowObject.addProperty(column, value.toString());
                }
            }
            dataArray.add(rowObject);
        }
        resultObject.add("data", dataArray);

        return resultObject.toString();*/

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