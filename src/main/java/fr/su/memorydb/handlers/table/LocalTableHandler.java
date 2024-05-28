package fr.su.memorydb.handlers.table;

import fr.su.memorydb.controllers.TableController;
import fr.su.memorydb.controllers.TableController.TableBody;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.utils.ToolBox;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.List;

@Singleton
public class LocalTableHandler implements TableHandler {

    @ConfigProperty(name = "fr.su.servers.ips")
    List<String> ips;

    @ConfigProperty(name = "fr.su.enable.values-compression")
    boolean enableValuesCompression;

    @ConfigProperty(name = "fr.su.enable.indexes-compression")
    boolean enableIndexesCompression;

    @ConfigProperty(name = "fr.su.enable.indexing")
    boolean enableIndexing;

    @ConfigProperty(name = "fr.su.blocs.size")
    int blocsSize;

    @Override
    public void createTable(TableBody tableBody, String server_id) {
        Database database = Database.getInstance();
        Table table = new Table(tableBody.getTableName());

        // This part decides which columns of the parquet file is stored in this server based on the list of ips
        int nbColumns = tableBody.getColumns().size();
        int nbIps = ips.size();
        int id = 0;
        if (server_id != null)
            id = Integer.parseInt(server_id);
        int nbFields = 0;
        int startingIndex = 0;

        if (id == 0)
            nbFields = (int) Math.ceil(nbColumns / (nbIps * 1.0));
        else
            nbFields = (int) Math.floor(nbColumns / (nbIps * 1.0));

        for (int i = 0; i < id; i++) {
            if (i == 0)
                startingIndex += (int) Math.ceil(nbColumns / (nbIps * 1.0));
            else
                startingIndex += (int) Math.floor(nbColumns / (nbIps * 1.0));
        }

        // Adding all columns and effectively stored columns
        for(int i = 0; i < tableBody.getColumns().size(); i++) {
            TableController.TableParameter tableParameter = tableBody.getColumns().get(i);
            boolean stored = i >= startingIndex && i < startingIndex + nbFields;
            Column newColumn = null;
            switch (tableParameter.getType().toLowerCase()) {
                case "boolean":
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, Boolean.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
                    break;
                case "int32":
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, Integer.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
                    break;
                case "int64":
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, Long.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
                    break;
                case "float":
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, Float.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
                    break;
                case "double":
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, Double.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
                    break;
                default:
                    newColumn = new Column<>(table, tableParameter.getName(), stored, blocsSize, String.class, enableValuesCompression, enableIndexesCompression, enableIndexing);
            }
            table.addColumn(newColumn);
            if (stored) {
                HashMap<Column, Integer> tmp = ToolBox.columnsRepartition.computeIfAbsent(tableBody.getTableName(), k -> new HashMap<>());
                tmp.put(newColumn, id);
            }
        }

        database.addTable(table);
    }

}
