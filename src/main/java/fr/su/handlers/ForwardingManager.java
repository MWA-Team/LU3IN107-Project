package fr.su.handlers;

import fr.su.controllers.TableInsertion;
import fr.su.proxy.TableInsertionProxy;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Supplier;

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
        String remoteHostAddr = context.request().remoteAddress().hostAddress();
        String localAddr = context.request().localAddress().hostAddress();

        if (!shouldForward(remoteHostAddr, localAddr)) {
            return "Query is coming from another server !";
        }
        System.out.println(localAddr + "\n" + remoteHostAddr);
        HttpServerRequest request = context.request();
        String uri = request.uri();
        var charset = request.getParamsCharset();
        Integer id = 2;
        for (String ip : ips) {
            if (ip.equals(localAddr))
                continue;
            else {
                URI newUri = URI.create("http://" + ip + ":8080");
                TableInsertionProxy proxy = RestClientBuilder.newBuilder().baseUri(newUri).build(TableInsertionProxy.class);
                proxy.insert(id.toString(), body);
                id++;
            }
        }
        return "Ok boomer";
    }

    private boolean shouldForward(String remoteHostAddr, String localAddr) {
        for (String ip : ips) {
            if (ip.equals(remoteHostAddr) && !ip.equals(localAddr)) {
                return false;
            }
        }
        return true;
    }

    /**
     * This function is used to get the local address of one of the computer's network interfaces if it is contained in the config ips.
     * @param
     * @return String
     * @throws SocketException
     */
    private String getSelfIP() throws SocketException {
        for (String ip : ips) {
            Enumeration<NetworkInterface> netInts = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface ni : Collections.list(netInts)) {
                Enumeration<InetAddress> inetAdresses = ni.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAdresses)) {
                    if (addr.getHostAddress().equals(ip))
                        return ip;
                }
            }
        }
        return null;
    }

}
