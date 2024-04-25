package fr.su.handlers.table;

import fr.su.controllers.TableController;
import fr.su.controllers.TableController.TableBody;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
        int id = 1;
        String server_id = context.request().params().get("server_id");
        if (server_id != null)
            id = Integer.parseInt(server_id);

        int nbFields = 0;
        int startingIndex = 0;

        if (id == 1)
            nbFields = (int) Math.ceil(nbColumns / (nbIps * 1.0));
        else
            nbFields = (int) Math.floor(nbColumns / (nbIps * 1.0));

        for (int i = 1; i < id; i++) {
            if (i == 1)
                startingIndex += (int) Math.ceil(nbColumns / (nbIps * 1.0));
            else
                startingIndex += (int) Math.floor(nbColumns / (nbIps * 1.0));
        }

        // Adding all columns and effectively stored columns
        for(int i = 0; i < tableBody.getColumns().size(); i++) {
            TableController.TableParameter tableParameter = tableBody.getColumns().get(i);
            boolean stored = i >= startingIndex && i < startingIndex + nbFields;
            Column newColumn = new Column(tableParameter.getName(), String.class, stored);
            table.getColumns().put(tableParameter.getName(), newColumn);
        }

        System.out.println("Creating table " + table.getName());
        System.out.println("Table columns size : " + table.getColumns().size());

        database.addTable(table);
        return tableBody;
    }

}
