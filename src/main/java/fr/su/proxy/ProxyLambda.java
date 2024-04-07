package fr.su.proxy;

import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.QueryParam;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface ProxyLambda {

    Object call(ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, String body);

}
