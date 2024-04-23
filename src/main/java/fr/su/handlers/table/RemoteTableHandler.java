package fr.su.handlers.table;

import fr.su.controllers.TableController.TableBody;
import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

@Singleton
public class RemoteTableHandler implements TableHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public Response createTable(TableBody tableBody) throws IOException {
        return forwardingManager.forwardCreate(tableBody);
    }
}
