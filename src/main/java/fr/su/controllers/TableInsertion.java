package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

@Path("insert")
public class TableInsertion {

    @Inject
    ForwardingManager forwardingManager;

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("file")
    public String getFile(InputStream inputStream) throws IOException {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    @GET
    public Object getTest() throws SocketException, UnknownHostException {
        return forwardingManager.forwardGet();
    }
}
