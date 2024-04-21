package fr.su.proxy;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.io.File;
import java.io.InputStream;

@RegisterRestClient
public interface ForwardingProxy {

    @POST
    // @Produces("...")
    @Consumes(MediaType.APPLICATION_JSON)
    Response post(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, File body);

    @GET
    // Produces("...")
    Response get(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @PUT
    // Produces("...")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    Response put(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

}
