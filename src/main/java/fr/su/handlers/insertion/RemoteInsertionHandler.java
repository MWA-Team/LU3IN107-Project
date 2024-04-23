package fr.su.handlers.insertion;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.File;
import java.io.IOException;

@Singleton
public class RemoteInsertionHandler implements InsertionHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public int insert(File file) throws IOException {

        forwardingManager.forwardInsert(file);

        return 200; // code par defaut à changer
    }
}
