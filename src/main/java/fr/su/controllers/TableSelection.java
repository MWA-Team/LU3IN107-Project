package fr.su.controllers;

import fr.su.database.Database;
import fr.su.database.Table;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;

@Path("select")
public class TableSelection{

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String selectColumns(String jsonBody) {
        Database database = Database.getInstance();

        JsonObject jsonObject = JsonParser.parseString(jsonBody).getAsJsonObject();
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

        return resultObject.toString();
    }
}