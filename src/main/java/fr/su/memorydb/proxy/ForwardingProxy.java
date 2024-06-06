package fr.su.memorydb.proxy;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.io.File;

@RegisterRestClient
public interface ForwardingProxy {

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Response create(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    Response insert(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, File body);

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Response rows(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Response where(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    Response memoryUsage(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

}
