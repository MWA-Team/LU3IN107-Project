package fr.su.memorydb.handlers.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.memorydb.controllers.TableSelection.SelectBody;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class RemoteSelectHandler implements SelectHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public SelectResponse select(SelectBody selectBody) throws IOException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(selectBody);
        Response response = forwardingManager.forwardSelect(json);

        if (response == null)
            return null;

        List<Response> responses = (List<Response>) response.getEntity();

        if (responses.isEmpty())
            return null;

        List<SelectResponse> retval = new LinkedList<>();

        ObjectMapper om = new ObjectMapper();
        for (Response r : responses) {
            if (r.getStatus() != 200)
                continue;
            retval.add(om.readValue(r.readEntity(String.class), SelectResponse.class));
        }

        if (retval.isEmpty())
            return null;

        SelectResponse last = retval.remove(retval.size() - 1);
        return last.merge(retval);
    }
}
