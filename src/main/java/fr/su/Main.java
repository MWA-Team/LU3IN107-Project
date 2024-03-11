package fr.su;

import fr.su.servers.SQLServer;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static List<SQLServer> servers = Arrays.asList(

            //Les différents serveurs seront ici
            //Il faut une méthode pour récupérer l'adresse actuelle du serveur comme ça on pourra comparer dans la liste les serveurs

    );

    public static boolean isActualServer(InetAddress address) {

        return true;
    }


}
