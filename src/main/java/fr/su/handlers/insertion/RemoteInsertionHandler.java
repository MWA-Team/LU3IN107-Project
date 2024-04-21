package fr.su.handlers.insertion;

import fr.su.handlers.ForwardingManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

@ApplicationScoped
public class RemoteInsertionHandler implements InsertionHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public int insert(File file) throws IOException {

        forwardingManager.forwardPost(file);

        return 200; //doit retourner 200 si toutes les requêtes "forwarded" ont retourné 200
    }
}
