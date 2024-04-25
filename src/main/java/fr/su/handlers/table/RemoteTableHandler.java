package fr.su.handlers.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
    public TableBody createTable(TableBody tableBody) throws IOException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(tableBody);
        Response response = forwardingManager.forwardCreate(json);
        return response.getStatus() != 200 ? null : (TableBody) response.getEntity();
    }
}
