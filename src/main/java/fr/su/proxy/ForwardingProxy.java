package fr.su.proxy;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient
public interface ForwardingProxy {

    @POST
    // @Produces("...")
    @Consumes(MediaType.APPLICATION_JSON)
    String post(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @GET
    // Produces("...")
    Object get(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @PUT
    // Produces("...")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    Object put(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

}
