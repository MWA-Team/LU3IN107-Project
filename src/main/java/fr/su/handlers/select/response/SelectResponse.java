package fr.su.handlers.select.response;

import com.google.gson.*;
import fr.su.controllers.TableSelection;
import fr.su.database.Column;
import com.google.gson.JsonArray;
import fr.su.database.Database;

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

    public JsonObject mergeJson(List<SelectResponse> responses, TableSelection.SelectBody selectBody) {
        JsonObject merged = this.toJson(selectBody);
        JsonArray data = merged.getAsJsonArray("data");
        if (data == null)
            return null;

        // Check if current server add to do a select
        if (data.isEmpty()) {
            for (Column column : Database.getInstance().getTables().get("test").getColumns().values()) {
                if (column.stored() && selectBody.getColumns().contains(column))
                    return merged;
            }
        }

        // Adding all indexes that match both
        for (SelectResponse response : responses) {
            if (response == null)
                continue;
            JsonObject rJson = response.toJson(selectBody);
            JsonArray rData = rJson.getAsJsonArray("data");
            if (rData == null)
                continue;

            boolean present = false;
            for (int i = 0; i < data.size(); i++) {
                JsonObject object = data.get(i).getAsJsonObject();
                Long index = object.get("index").getAsLong();
                for (int j = 0; j < rData.size(); j++) {
                    JsonObject rObject = rData.get(j).getAsJsonObject();
                    Long rIndex = rObject.get("index").getAsLong();
                    if (index == rIndex) {
                        present = true;
                        for (String columnName : selectBody.getColumns()) {
                            JsonElement tmp = rObject.get(columnName);
                            if (tmp == null)
                                continue;
                            object.addProperty(columnName, tmp.getAsString());
                        }
                        break;
                    }
                }
                if (!present)
                    data.remove(i);
            }
        }
        return merged;
    };

    public SelectResponse merge(List<SelectResponse> responses, TableSelection.SelectBody selectBody) {
        SelectResponse merged = new SelectResponse();

        // Check if current servers had to do a select
        if (indexes.isEmpty()) {
            for (Column column : Database.getInstance().getTables().get("test").getColumns().values()) {
                if (column.stored() && selectBody.getColumns().contains(column))
                    return merged;
            }
        }

        merged.indexes.addAll(indexes);

        // Managing indexes
        for (SelectResponse response : responses) {
            if (response == null || response.indexes.isEmpty())
                continue;
            for (Long index : response.indexes) {
                if (!merged.indexes.isEmpty() && !merged.indexes.contains(index))
                    merged.indexes.remove(index);
                else
                    merged.indexes.add(index);
            }
        }

        // Managing columns
        for (SelectResponse response : responses) {
            for (Column column : Database.getInstance().getTables().get("test").getColumns().values()) {
                Column newColumn = new Column(column.getName(), String.class, true);
                for (Column tmp : response.columns) {
                    if (column.getName().equals(tmp.getName())) {
                        for(Long i : merged.indexes) {
                            newColumn.addValue(i, tmp.getValues().get(i).toString());
                        }
                    }
                }
                merged.columns.add(newColumn);
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

    public static SelectResponse fromJson(JsonObject json) {
        SelectResponse retval = new SelectResponse();
        JsonArray data = json.getAsJsonArray("data");
        if (data == null)
            return retval;

        for (JsonElement element : data) {
            JsonObject tmp = element.getAsJsonObject();
            Long index = tmp.get("index").getAsLong();

            for (Column column : Database.getInstance().getTables().get("test").getColumns().values()) {
                boolean skip = true;
                for (Column retcol : retval.columns) {
                    if (retcol.getName().equals(column.getName())) {
                        retcol.addValue(index, tmp.get(column.getName()).toString());
                        skip = false;
                    }
                }
                retval.indexes.add(index);
                if (skip)
                    continue;
                Column newColumn = new Column(column.getName(), String.class, true);
                newColumn.addValue(index, tmp.get(column.getName()).toString());
                retval.columns.add(newColumn);
            }
        }

        return retval;
    }

}
