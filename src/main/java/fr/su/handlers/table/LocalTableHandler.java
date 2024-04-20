package fr.su.handlers.table;

import fr.su.controllers.TableController;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;

import java.util.UUID;

public class LocalTableHandler implements TableHandler {
    @Override
    public void createTable(TableController.TableBody tableBody) {

        Database database = Database.getInstance();

        Table table = new Table(tableBody.getTableName());
        System.out.println("Creating table " + table.getName());
        System.out.println("Table columns size : " + table.getColumns().size());

        for(TableController.TableParameter tableParameter : tableBody.getColumns()) {

            table.getColumns().put(tableParameter.getName(), new Column(tableParameter.getName(), String.class));
        }

        System.out.println("Table columns size : " + table.getColumns().size());

        database.addTable(table);
    }
}
