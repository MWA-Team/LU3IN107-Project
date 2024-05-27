package fr.su.memorydb.handlers;

import fr.su.memorydb.proxy.ForwardingProxy;
import fr.su.memorydb.utils.lambda.ProxyLambda;

import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.*;

@Singleton
public class ForwardingManager {

    @Context
    RoutingContext context;

    @ConfigProperty(name = "fr.su.servers.ips")
    List<String> ips;

    public ForwardingManager() {}

    /**
     * This function is used to forward a POST query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardInsert(File body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> {
            return proxy.insert(signature, id, (File) data);
        });
    }

    /**
     * This function is used to forward a PUT query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardCreate(String body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> {
            return proxy.create(signature, id, data.toString());
        });
    }

    /**
     * This function is used to forward a GET query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardSelect(String body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> {
            return proxy.select(signature, id, data.toString());
        });
    }

    /**
     * This function is used to forward a query to the other servers on the distributed system.
     * It recognizes its own addresses and skip them in order to not make a loop.
     * With the same idea, it doesn't forward the query if the sender is another server.
     * @param body contains the body of the query
     * @param lambda is the function that needs to be called in ForwardingProxy (used to factorize code)
     * @return Object
     * @throws IOException
     */
    private Response forwardQuery(Object body, ProxyLambda lambda) throws IOException {
        /*
        All server use a header as a signature. If this signature isn't found, we can assume that a client made the query.
         */
        String serverSignature = context.request().headers().get("Server-Signature");
        if (serverSignature != null && !serverSignature.isEmpty()) {
            return null;
        }

        int id = 1;
        String localAddr = context.request().localAddress().hostAddress();
        List<Response> responses = new ArrayList<>();
        for (String ip : ips) {
            if (isLocalMachine(ip))
                continue;
            else {
                System.out.println("Forwarding happened once.");
                URI newUri = URI.create("http://" + ip + ":8080" + context.request().uri());
                ForwardingProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(ForwardingProxy.class);
                Response r = lambda.call(proxy, localAddr, Integer.toString(id), body);
                responses.add(r);
                id++;
            }
        }
        
        List<Response> entities = new LinkedList<>(responses);
        Response response = Response.status(200).entity(entities).build();
        return response;
    }

    /**
     * This function is used to know if the given address is one of the computer's network interfaces' IP address.
     * @param testIp
     * @return boolean
     * @throws SocketException
     */
    public boolean isLocalMachine(String testIp) throws SocketException {
        Enumeration<NetworkInterface> inets = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface inet : Collections.list(inets)) {
            Enumeration<InetAddress> addrs = inet.getInetAddresses();
            for (InetAddress addr : Collections.list(addrs)) {
                if (addr.getHostAddress().equals(testIp))
                    return true;
            }
        }
        return false;
    }

    public String getLocalIp() throws SocketException {

        for (String ip : ips) {
            if (isLocalMachine(ip))
                return ip;
        }

        return null;
    }

}
