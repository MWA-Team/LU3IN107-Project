package fr.su.memorydb.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.DebugRowsResponse;
import fr.su.memorydb.utils.response.MemoryUsageResponse;
import fr.su.memorydb.utils.response.RepartitionResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Path("debug")
public class TableDebug {

    @Context
    RoutingContext routingContext;

    @Inject
    ForwardingManager forwardingManager;

    @Inject
    ToolBox toolBox;

    @GET
    @Path("repartition")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRepartition() {
        System.gc();
        Instant start = Instant.now();

        HashMap<String, HashMap<String, String>> repartition = new HashMap<>();
        for (Map.Entry<String, HashMap<String, Integer>> entry : toolBox.columnsRepartition.entrySet()) {
            HashMap<String, String> map = repartition.computeIfAbsent(entry.getKey(), k -> new HashMap<>());
            for (Map.Entry<String, Integer> repartitionEntry : entry.getValue().entrySet()) {
                map.put(repartitionEntry.getKey(), toolBox.ips()[repartitionEntry.getValue()]);
            }
        }

        return Response.status(200).entity(new RepartitionResponse(repartition).details("The repartition of the tables on the different servers.").setStart(start).done()).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("memory-usage")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMemoryUsage() throws IOException, InterruptedException {
        System.gc();
        Instant start = Instant.now();

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        HashMap<String, Double> memoryUsage = new HashMap<>();
        Runtime r = Runtime.getRuntime();
        double usage = (double) r.freeMemory() * 100 / r.maxMemory();
        memoryUsage.put(toolBox.ips()[toolBox.server_id() != null ? Integer.parseInt(toolBox.server_id()) : 0], usage);

        Response remoteResponse;
        if (toolBox.serverSignature() == null) {
            remoteResponse = forwardingManager.forwardMemoryUsage();
            HashMap<Integer, Response> responses = (HashMap<Integer, Response>) remoteResponse.getEntity();
            ObjectMapper om = new ObjectMapper();
            om.registerModule(new JavaTimeModule());
            for (Map.Entry<Integer, Response> entry : responses.entrySet()) {
                if (entry.getValue().getStatus() != 200)
                    continue;
                String t = entry.getValue().readEntity(String.class);
                memoryUsage.putAll(om.readValue(t, MemoryUsageResponse.class).getMemoryUsage());
            }
        }

        return Response.status(200).entity(new MemoryUsageResponse(memoryUsage).details("The memory usage on the different servers.").setStart(start).done()).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("rows")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRows() {
        System.gc();
        Instant start = Instant.now();

        HashMap<String, Integer> rows = new HashMap<>();
        for (Map.Entry<String, Table> entry : Database.getInstance().getTables().entrySet()) {
            rows.put(entry.getKey(), entry.getValue().rowsCounter);
        }

        return Response.status(200).entity(new DebugRowsResponse(rows).details("How many rows are present in each table.").setStart(start).done()).type(MediaType.APPLICATION_JSON).build();
    }

}
