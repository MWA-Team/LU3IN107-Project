package fr.su.servers;

import java.net.InetAddress;
import java.util.UUID;

public class SQLServer {

    private String name;
    private InetAddress address;
    private UUID uuid;

    public SQLServer(String name, InetAddress address) {

        this.name = name;
        this.address = address;
        this.uuid = UUID.randomUUID();
    }
}
