package fr.su.handlers.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import fr.su.controllers.TableSelection;
import fr.su.handlers.ForwardingManager;
import fr.su.handlers.select.response.SelectResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;

@Singleton
public class RemoteSelectHandler implements SelectHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) throws IOException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(selectBody);
        forwardingManager.forwardSelect(json);

        return null;
    }
}
