package fr.su.memorydb.handlers.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.RowsResponse;
import fr.su.memorydb.utils.response.SelectResponse;
import fr.su.memorydb.utils.response.WhereResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Singleton
public class RemoteSelectHandler implements SelectHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public int[] where(TableSelection.WhereBody whereBody) {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = null;
        try {
            json = ow.writeValueAsString(whereBody);
            Response response = forwardingManager.forwardWhere(json);
            if (response == null)
                return null;

            HashMap<Integer, Response> responses = (HashMap<Integer, Response>) response.getEntity();
            if (responses.isEmpty())
                return null;

            List<int[]> retval = new LinkedList<>();
            ObjectMapper om = new ObjectMapper();
            for (Map.Entry<Integer, Response> entry : responses.entrySet()) {
                if (entry.getValue().getStatus() != 200)
                    continue;
                String t = entry.getValue().readEntity(String.class);
                retval.add(om.readValue(t, WhereResponse.class).getIndexes());
            }

            if (retval.isEmpty())
                return null;

            return WhereResponse.mergeIndexes(retval);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<HashMap<String, Object>> select(TableSelection.SelectBody selectBody, int[] indexes) throws IOException, InterruptedException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(new TableSelection.RowsBody(selectBody, indexes));
        Response response = forwardingManager.forwardRows(json);

        if (response == null)
            return null;

        HashMap<Integer, Response> responses = (HashMap<Integer, Response>) response.getEntity();

        if (responses.isEmpty())
            return null;

        List<List<HashMap<String, Object>>> rows = new LinkedList<>();

        ObjectMapper om = new ObjectMapper();
        for (Map.Entry<Integer, Response> entry : responses.entrySet()) {
            if (entry.getValue().getStatus() != 200)
                continue;
            String t = entry.getValue().readEntity(String.class);
            RowsResponse tmp = om.readValue(t, RowsResponse.class);
            rows.add(tmp.getRows());
        }

        if (rows.isEmpty())
            return null;

        return RowsResponse.naiveMergeRows(rows);
    }

}
