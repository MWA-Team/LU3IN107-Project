package fr.su.memorydb.handlers.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.memorydb.controllers.TableController.TableBody;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.utils.response.TableResponse;
import fr.su.memorydb.utils.ToolBox;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Singleton
public class RemoteTableHandler implements TableHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public void createTable(TableBody tableBody) throws IOException, InterruptedException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(tableBody);
        Response response = forwardingManager.forwardCreate(json);
        if (response == null || response.getStatus() != 200)
            // Throw exception
            return;
        HashMap<Integer, Response> responses = (HashMap<Integer, Response>) response.getEntity();
        if (responses.isEmpty())
            // Throw exception
            return;

        ObjectMapper om = new ObjectMapper();
        // Getting which Column is stored on which server
        for (Map.Entry<Integer, Response> entry : responses.entrySet()) {
            TableResponse tr = om.readValue(entry.getValue().readEntity(String.class), TableResponse.class);
            for (String column : tr.getColumns()) {
                HashMap<String, Integer> tmp = ToolBox.columnsRepartition.get(tr.getTable());
                tmp.put(column, entry.getKey());
            }
        }
    }
}
