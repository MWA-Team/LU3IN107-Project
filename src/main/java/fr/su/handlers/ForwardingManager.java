package fr.su.handlers;

import fr.su.proxy.ForwardingProxy;
import fr.su.proxy.ProxyLambda;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

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
    public Object forwardPost(String body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, String data) -> {
            return proxy.post(signature, id, data);
        });
    }

    /**
     * This function is used to forward a PUT query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Object forwardPut(String body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, String data) -> {
            return proxy.put(signature, id, data);
        });
    }

    /**
     * This function is used to forward a GET query to the other servers on the distributed system.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Object forwardGet(String body) throws IOException {
        return forwardQuery(body, (ForwardingProxy proxy, @HeaderParam("Server-Signature") String signature, @QueryParam("server_id") String id, String data) -> {
            return proxy.get(signature, id, data);
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
    private Object forwardQuery(String body, ProxyLambda lambda) throws IOException {
        /*
        All server use a header as a signature. If this signature isn't found, we can assume that a client made the query.
         */
        String serverSignature = context.request().headers().get("Server-Signature");
        if (serverSignature != null && !serverSignature.isEmpty()) {
            return "Query is coming from another server !";
        }

        Integer id = 2;
        String localAddr = context.request().localAddress().hostAddress();
        List<Object> retval = new ArrayList<>();
        for (String ip : ips) {
            if (isLocalMachine(ip))
                continue;
            else {
                System.out.println("Forwarding happened once.");
                URI newUri = URI.create("http://" + ip + ":8080" + context.request().uri());
                ForwardingProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(ForwardingProxy.class);
                retval.add(lambda.call(proxy, localAddr, id.toString(), body));
                id++;
            }
        }
        return "Query from \"reel client\" and forwarding may have happened (refer to above logs)";
    }

    /**
     * This function is used to know if the given address is one of the computer's network interfaces' IP address.
     * @param testIp
     * @return boolean
     * @throws SocketException
     */
    private boolean isLocalMachine(String testIp) throws SocketException {
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

}
