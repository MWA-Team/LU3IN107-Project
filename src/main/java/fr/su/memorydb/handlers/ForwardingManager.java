package fr.su.memorydb.handlers;

import fr.su.memorydb.proxy.ForwardingProxy;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.lambda.ProxyLambda;

import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Produces;
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

    @Inject
    ToolBox toolBox;

    public ForwardingManager() {}

    /**
     * This function is used to forward a POST query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardInsert(File body) throws IOException, InterruptedException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> proxy.insert(signature, id, (File) data));
    }

    /**
     * This function is used to forward a PUT query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardCreate(String body) throws IOException, InterruptedException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> proxy.create(signature, id, data.toString()));
    }

    /**
     * This function is used to forward a GET query to the other servers on the distributed system in order to get filtered indexes by a "where".
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardWhere(String body) throws IOException, InterruptedException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> proxy.where(signature, id, data.toString()));
    }

    /**
     * This function is used to forward a GET query to the other servers on the distributed system in order to get all rows filtered by an array of indexes.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Response forwardRows(String body) throws IOException, InterruptedException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> proxy.rows(signature, id, data.toString()));
    }

    public Response forwardMemoryUsage() throws IOException, InterruptedException {
        return forwardQuery(null, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, Object data) -> proxy.memoryUsage(signature, id, null));
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
    private Response forwardQuery(Object body, ProxyLambda lambda) throws IOException, InterruptedException {
        String[] ips = toolBox.ips();
        /*
        All server use a header as a signature. If this signature isn't found, we can assume that a client made the query.
         */
        if (toolBox.serverSignature() != null && !toolBox.serverSignature().isEmpty()) {
            return null;
        }

        int id = 1;
        String localAddr = getLocalIp();
        List<Thread> threads = new ArrayList<>(ips.length - 1);
        HashMap<Integer, Response> responses = new HashMap<>();
        for (int i = 0; i < ips.length; i++) {
            String ip = ips[i];
            if (isLocalMachine(ip))
                continue;

            int finalId = id;
            Thread thread = new Thread(() -> {
                URI newUri = URI.create("http://" + ip + ":8080" + toolBox.uri());
                ForwardingProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(ForwardingProxy.class);
                Response r = lambda.call(proxy, localAddr, Integer.toString(finalId), body);
                responses.put(finalId, r);
            });

            id++;
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        return Response.status(200).entity(responses).build();
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
        for (String ip : toolBox.ips()) {
            if (isLocalMachine(ip))
                return ip;
        }
        return null;
    }

}
