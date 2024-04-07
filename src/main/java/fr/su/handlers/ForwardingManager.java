package fr.su.handlers;

import fr.su.proxy.TableInsertionProxy;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
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
     * This function is used to forward a GET query to the other servers on the system.
     * It recognizes its own address and skip it in order to not make a loop.
     * With the same idea, it doesn't forward the query if the sender is another server.
     * @param body
     * @return Object
     * @throws IOException
     */
    public Object forwardPost(String body) throws IOException {
        /*
        All server use a header as a signature. If this signature isn't found, we can assume that a client made the query.
         */
        if (context.request().headers().get("Server-Signature") != null) {
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
                URI newUri = URI.create("http://" + ip + ":8080");
                TableInsertionProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(TableInsertionProxy.class);
                retval.add(proxy.insert(localAddr, id.toString(), body));
                id++;
            }
        }
        return "Query from \"reel client\" and forwarding maybe have happened (refer to above logs)";
    }

    /**
     * This function is used to get the local address of one of the computer's network interfaces if it is contained in the config ips.
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
