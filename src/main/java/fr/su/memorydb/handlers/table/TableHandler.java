package fr.su.memorydb.handlers.table;

import fr.su.memorydb.controllers.TableController;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;

public interface TableHandler {

    void createTable(TableController.TableBody tableBody, String server_id) throws IOException, InterruptedException;

}
