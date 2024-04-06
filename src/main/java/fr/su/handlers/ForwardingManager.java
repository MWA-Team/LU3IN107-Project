package fr.su.handlers;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Singleton
public class ForwardingManager {

    @Context
    RoutingContext context;

    @ConfigProperty(name = "fr.su.servers.ips")
    List<String> ips;

    @ConfigProperty(name = "fr.su.query.servers.id")
    String server_id;

    public ForwardingManager() {}

    /**
     * This function is used to forward a GET query to the other servers on the system.
     * It recognizes its own address and skip it in order to not make a loop.
     * With the same idea, it doesn't forward the query if the sender is another server.
     * @param
     * @return
     * @throws SocketException
     */
    public Object forwardPost(InputStream body) throws IOException, InterruptedException {
        String remoteHostAddr = context.request().remoteAddress().hostAddress();
        String localAddr = context.request().localAddress().hostAddress();

        if (!shouldForward(remoteHostAddr, localAddr)) {
            return "Query is coming from another server !";
        }

        HttpServerRequest request = context.request();
        String uri = request.uri();
        var charset = request.getParamsCharset();
        int id = 2;
        List<String> retval = new ArrayList<>();
        for (String ip : ips) {
            if (ip.equals(localAddr))
                continue;
            else {
                //context.request().params().set("key1", "value1");
                URI newAbsUri = URI.create("http://" + ip + ":8080" + uri);

                HttpRequest newRequest = HttpRequest.newBuilder(newAbsUri)
                .headers("Content-Type", request.headers().get("Content-Type"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(new Supplier<InputStream>() {
                    @Override
                    public InputStream get() {
                        return body;
                    }
                })).build();
                System.out.println();
                retval.add(HttpClient.newHttpClient().send(newRequest, HttpResponse.BodyHandlers.ofString()).body());
            }
        }
        return retval;
    }

    private boolean shouldForward(String remoteHostAddr, String localAddr) {
        for (String ip : ips) {
            if (ip.equals(remoteHostAddr) && !ip.equals(localAddr)) {
                return false;
            }
        }
        return true;
    }

}
