package fr.su.handlers.select;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import fr.su.controllers.TableController;
import fr.su.controllers.TableSelection.SelectBody;
import fr.su.handlers.ForwardingManager;
import fr.su.handlers.select.response.SelectResponse;
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
        if (response == null || response.getStatus() != 200)
            return null;
        List<Response> responses = (List<Response>) response.getEntity();
        if (responses.size() == 0)
            return null;

        List<SelectResponse> retval = new LinkedList<>();

        ObjectMapper om = new ObjectMapper();
        for (Response r : responses) {
            retval.add(om.readValue(r.readEntity(String.class), SelectResponse.class));
        }
        SelectResponse last = retval.remove(retval.size() - 1);
        return last.merge(retval, selectBody);
    }
}
