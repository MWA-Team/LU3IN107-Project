package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

@Path("insert")
public class TableInsertion {

    @Inject
    ForwardingManager forwardingManager;

    /*@POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("file")
    public String getFile(InputStream inputStream) throws IOException {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }*/

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Object getTest(InputStream body) throws IOException, InterruptedException {
        return forwardingManager.forwardPost(body);
    }

}
