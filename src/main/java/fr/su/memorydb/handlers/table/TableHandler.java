package fr.su.memorydb.handlers.table;

import fr.su.memorydb.controllers.TableController;

import java.io.IOException;

public interface TableHandler {

    public TableController.TableBody createTable(TableController.TableBody tableBody) throws IOException;

}
