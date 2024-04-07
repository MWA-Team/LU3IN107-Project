package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;

@Path("/insert")
public class TableInsertion {

    @Inject
    ForwardingManager forwardingManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Object getFile(String body) throws IOException {
        return forwardingManager.forwardPost(body);
    }

}
