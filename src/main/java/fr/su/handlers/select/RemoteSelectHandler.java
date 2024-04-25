package fr.su.handlers.select;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.controllers.TableSelection.SelectBody;
import fr.su.handlers.ForwardingManager;
import fr.su.handlers.select.response.SelectResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
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
        Object entity = response.getStatus() != 200 ? null : response.getEntity();
        List<SelectResponse> list = (List<SelectResponse>) entity;
        SelectResponse last = list.get(list.size() - 1);
        list.remove(list.size() - 1);
        return last.merge(list);
    }
}
