package fr.su.handlers.select.response;

import fr.su.controllers.TableSelection;
import fr.su.database.Column;
import fr.su.database.Database;

import java.util.*;

public class SelectResponse {

    private Set<Integer> indexes;
    private List<Column> columns;

    public SelectResponse() {
        this.indexes = new HashSet<>();
        this.columns = new ArrayList<>();
    }

    public Set<Integer> getIndexes() { return indexes; }

    public List<Column> getColumns() { return columns; }

    public SelectResponse merge(List<SelectResponse> responses, TableSelection.SelectBody selectBody) {
        SelectResponse merged = new SelectResponse();

        if(responses == null) { return this; }

        // Check if current servers had to do a select with condition
        if (indexes.isEmpty()) {
            for (Column column : Database.getInstance().getTables().get("test").getColumns()) {
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
                boolean present = false;
                Column current = null;
                for (Column tmp : merged.columns) {
                    if (tmp.getName().equals(column.getName())) {
                        present = true;
                        current = tmp;
                        break;
                    }
                }
                if (!present) {
                    current = new Column(column.getName(), true, column.getType());
                    merged.columns.add(current);
                }

                // Getting correct values corresponding to indexes
                for (Integer i : merged.indexes) {
                    Object o = column.getValues();
                    if (o != null)
                        current.addRowValue(o, i);
                }
            }
        }

        return merged;
    };

}
