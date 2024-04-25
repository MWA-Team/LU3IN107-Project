package fr.su.handlers.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.controllers.TableController.TableBody;
import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;

@Singleton
public class RemoteTableHandler implements TableHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public TableBody createTable(TableBody tableBody) throws IOException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(tableBody);
        Response response = forwardingManager.forwardCreate(json);
        if (response == null)
            return tableBody;
        // Attention, TO DO : parse the response as it is a list of objects
        List<TableBody> res2 = (List<TableBody>) response.getEntity();

        return response.getStatus() != 200 || res2.isEmpty() ? null : res2.get(0);
    }
}
