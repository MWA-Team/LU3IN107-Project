package fr.su.proxy;

import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@FunctionalInterface
public interface ProxyLambda {

    Response call(ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object body);

}
