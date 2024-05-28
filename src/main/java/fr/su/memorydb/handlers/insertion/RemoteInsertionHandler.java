package fr.su.memorydb.handlers.insertion;

import fr.su.memorydb.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;

@Singleton
public class RemoteInsertionHandler implements InsertionHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public int insert(File file, String table) throws IOException {
        Response response = forwardingManager.forwardInsert(file);
        return response == null ? 200 : response.getStatus();
    }

}
