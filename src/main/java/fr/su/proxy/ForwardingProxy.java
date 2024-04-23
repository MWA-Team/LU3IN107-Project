package fr.su.proxy;
import fr.su.controllers.TableController.TableBody;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.io.File;

@RegisterRestClient
public interface ForwardingProxy {

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    Response insert(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, File body);

    @GET
    Response select(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    Response create(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, TableBody body);

}
