package fr.su.handlers.insertion;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.InputStream;

public class RemoteInsertionHandler implements InsertionHandler {

    @Inject
    ForwardingManager forwardingManager;

    @Override
    public int insert(InputStream inputStream) throws IOException {

        //forwardingManager.forwardPost(inputStream.toString()); //à changer, forwardPost doit accepter un InputStream

        return 200; //doit retourner 200 si toutes les requêtes "forwarded" ont retourné 200
    }
}
