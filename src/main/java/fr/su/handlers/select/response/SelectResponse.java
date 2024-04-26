package fr.su.handlers.select.response;

import com.google.gson.*;
import fr.su.controllers.TableSelection;
import fr.su.database.Column;
import com.google.gson.JsonArray;
import fr.su.database.Database;
import jakarta.transaction.Transactional;

import java.util.*;
import java.util.stream.Collectors;

public class SelectResponse {

    private Set<Long> indexes;
    private List<Column> columns;

    public SelectResponse() {
        this.indexes = new HashSet<>();
        this.columns = new ArrayList<>();
    }

    public Set<Long> getIndexes() { return indexes; }

    public List<Column> getColumns() { return columns; }

    public SelectResponse merge(List<SelectResponse> responses, TableSelection.SelectBody selectBody) {
        SelectResponse merged = new SelectResponse();

        if(responses == null) { return this; }

        // Check if current servers had to do a select with condition
        if (indexes.isEmpty()) {
            for (Column column : Database.getInstance().getTables().get("test").getColumns().values()) {
                if (column.stored() && selectBody.getColumns().contains(column))
                    return merged;
            }
        }

        merged.indexes.addAll(indexes);
        merged.columns.addAll(this.columns);

        for (int z = 0; z < responses.size(); z++) {
            SelectResponse response = responses.get(z);
            if (response == null || response.indexes.isEmpty())
                continue;

            //Removing indexes that are not present in both response and merged
            for (Object i : merged.indexes.toArray()) {
                boolean present = false;
                for (Object j : response.indexes.toArray()) {
                    if (((Long) i).equals(j)) {
                        present = true;
                        break;
                    }
                }
                if (!present)
                    merged.indexes.remove(i);
            }

            // Addding all columns
            for (Column column : response.columns) {
                boolean presen                System.out.println(column.getName());
t = false;
                Column current = null;
                for (Column tmp : merged.columns) {
                    if (tmp.getName().equals(column.getName())) {
                        present = true;
                        current = tmp;
                        break;
                    }
                }
                if (!present) {
                    current = new Column(column.getName(), String.class, true);
                    merged.columns.add(current);
                }

                // Getting correct values corresponding to indexes
                for (Long i : merged.indexes) {
                    Object o = column.getValues().get(i);
                    if (o != null)
                        current.addValue(i, o.toString());
                }
            }
        }

        return merged;
    };

    public JsonObject toJson(TableSelection.SelectBody selectBody) {
        com.google.gson.JsonObject resultObject = new com.google.gson.JsonObject();
        JsonArray dataArray = new JsonArray();
        for (long i : getIndexes()) {
            com.google.gson.JsonObject rowObject = new com.google.gson.JsonObject();
            for (Column column : getColumns()) {
                if(!selectBody.getColumns().contains(column))
                    continue;
                Object value = column.getValues().get(i);
                rowObject.addProperty(column.getName(), value == null ? "null" : value.toString());
            }
            rowObject.addProperty("index", i);
            dataArray.add(rowObject);
        }
        resultObject.add("data", dataArray);
        return resultObject;
    }

}
