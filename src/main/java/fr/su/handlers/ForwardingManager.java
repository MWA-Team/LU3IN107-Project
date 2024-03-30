package fr.su.handlers;

import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

@ApplicationScoped
public class ForwardingManager {

    @ConfigProperty(name = "fr.su.servers.ips", defaultValue = "")
    List<String> ips;

    @Inject
    RoutingContext context;

    public ForwardingManager() { }

    /**
     * This function is used to forward a GET query to the other servers on the system.
     * It recognizes its own address and skip it in order to not make a loop.
     * With the same idea, it doesn't forward the query if the sender is another server.
     * @param
     * @return
     * @throws SocketException
     */
    public Object forwardGet() throws SocketException {
        String remoteHostAddr = context.request().remoteAddress().hostAddress();
        String localAddr = getSelfIP();

        if (!shouldForward(remoteHostAddr, localAddr)) {
            return "Query is coming from another server !";
        }

        for (String ip : ips) {
            if (ip.equals(localAddr))
                continue;
            else {
                // Forwarding GET using context
            }
        }
        return context.request();
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

    private boolean shouldForward(String remoteHostAddr, String localAddr) {
        for (String ip : ips) {
            if (ip.equals(remoteHostAddr) && !ip.equals(localAddr)) {
                return false;
            }
        }
        return true;
    }

}
