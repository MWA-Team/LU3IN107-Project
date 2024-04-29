package fr.su.handlers.table;

import fr.su.controllers.TableController;
import fr.su.controllers.TableController.TableBody;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigInteger;
import java.util.List;

@Singleton
public class LocalTableHandler implements TableHandler {

    @Context
    RoutingContext context;

    @ConfigProperty(name = "fr.su.servers.ips")
    List<String> ips;

    @Override
    public TableBody createTable(TableBody tableBody) {
        Database database = Database.getInstance();
        Table table = new Table(tableBody.getTableName());

        // This part decides which columns of the parquet file is stored in this server based on the list of ips
        int nbColumns = tableBody.getColumns().size();
        int nbIps = ips.size();
        int id = 0;
        String server_id = context.request().params().get("server_id");
        if (server_id != null)
            id = Integer.parseInt(server_id);
        // Can we use modulo instead ???
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
                    newColumn = new Column<Boolean>(tableParameter.getName(), stored);
                    break;
                case "int32":
                    newColumn = new Column<Integer>(tableParameter.getName(), stored);
                    break;
                case "int64":
                    newColumn = new Column<Long>(tableParameter.getName(), stored);
                    break;
                case "int96":
                    newColumn = new Column<BigInteger>(tableParameter.getName(), stored);
                    break;
                case "float":
                    newColumn = new Column<Float>(tableParameter.getName(), stored);
                    break;
                case "double":
                    newColumn = new Column<Double>(tableParameter.getName(), stored);
                    break;
                default:
                    newColumn = new Column<String>(tableParameter.getName(), stored);
            }
            table.getColumns().put(tableParameter.getName(), newColumn);
        }

        database.addTable(table);
        return tableBody;
    }

}
