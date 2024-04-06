package fr.su.proxy;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.logging.annotations.BaseUrl;

@Path("/insert")
@RegisterRestClient
public interface TableInsertionProxy {

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    void insert(@QueryParam("server_id") String id, String body);

}
