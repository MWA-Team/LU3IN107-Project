package fr.su.memorydb.utils;

import fr.su.memorydb.database.Column;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;

@Singleton
public class ToolBox {

    @ConfigProperty(name = "fr.su.servers.ips")
    String[] ips;

    @ConfigProperty(name = "fr.su.enable.values-compression")
    boolean enableValuesCompression;

    @ConfigProperty(name = "fr.su.enable.indexes-compression")
    boolean enableIndexesCompression;

    @ConfigProperty(name = "fr.su.enable.indexing")
    boolean enableIndexing;

    @ConfigProperty(name = "fr.su.blocs.size")
    int blocsSize;

    public static HashMap<String, HashMap<String, Integer>> columnsRepartition = new HashMap<>();
    private static Context context = new Context();

    public static class Context {

        private String uri;
        private String server_id;
        private String serverSignature;

        public Context() {
            uri = null;
            server_id = null;
            serverSignature = null;
        }

        public Context(String uri, String server_id, String serverSignature) {
            this.uri = uri;
            this.server_id = server_id;
            this.serverSignature = serverSignature;
        }

        public String uri() {
            return uri;
        }

        public String server_id() {
            return server_id;
        }

        public String serverSignature() {
            return serverSignature;
        }

    }

    public static void setContext(Context context) {
        ToolBox.context = context;
    }

    public String uri() {
        return context.uri();
    }

    public void setUri(String uri) {
        context.uri = uri;
    }

    public String server_id() {
        return context.server_id();
    }

    public String serverSignature() {
        return context.serverSignature();
    }

    public String[] ips() {
        return ips;
    }

    public boolean enableValuesCompression() {
        return enableValuesCompression;
    }

    public boolean enableIndexesCompression() {
        return enableIndexesCompression;
    }

    public boolean enableIndexing() {
        return enableIndexing;
    }

    public int blocsSize() {
        return blocsSize;
    }

}