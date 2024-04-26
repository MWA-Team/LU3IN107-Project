package fr.su.handlers.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.controllers.TableController.TableBody;
import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.json.Json;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.LinkedList;
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
        if (response == null || response.getStatus() != 200)
            return null;
        List<Response> responses = (List<Response>) response.getEntity();
        if (responses.size() == 0)
            return null;

        ObjectMapper om = new ObjectMapper();
        List<TableBody> retval = new LinkedList<>();
        for (Response r : responses) {
            retval.add(r.readEntity(TableBody.class));
        }
        return response.getStatus() != 200 || retval.isEmpty() ? null : retval.get(0);
    }
}
