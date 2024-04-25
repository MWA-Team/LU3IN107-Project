package fr.su.handlers.table;

import fr.su.controllers.TableController;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

public interface TableHandler {

    public TableController.TableBody createTable(TableController.TableBody tableBody) throws IOException;

}
