package fr.su.proxy;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/insert")
@RegisterRestClient
public interface TableInsertionProxy {

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object insert(@HeaderParam ("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

}
