package fr.su.handlers;

import fr.su.proxy.TableInsertionProxy;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.io.IOException;
import java.net.*;
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
     * @param
     * @return
     * @throws SocketException
     */
    public Object forwardPost(String body) throws IOException, InterruptedException {
        if (!shouldForward(context.request().remoteAddress().hostAddress())) {
            return "Query is coming from another server !";
        }

        System.out.println("local addr : " + context.request().localAddress().hostAddress() + " | " + "remote addr : " + context.request().remoteAddress().hostAddress());

        HttpServerRequest request = context.request();
        Integer id = 2;
        for (String ip : ips) {
            if (isLocalMachine(ip))
                continue;
            else {
                System.out.println("Forwarding happened once.");
                URI newUri = URI.create("http://" + ip + ":8080");
                TableInsertionProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(TableInsertionProxy.class);
                proxy.insert(id.toString(), body);

                id++;
            }
        }
        return "Query from \"reel client\" and forwarding maybe have happened";
    }

    private boolean shouldForward(String remoteHostAddr) throws SocketException {
        for (String ip : ips) {
            if (ip.equals(remoteHostAddr) && !isLocalMachine(ip)) {
                return false;
            }
        }
        return true;
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
