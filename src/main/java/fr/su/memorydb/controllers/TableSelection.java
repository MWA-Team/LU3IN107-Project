package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.handlers.select.LocalSelectHandler;
import fr.su.memorydb.handlers.select.RemoteSelectHandler;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.RowsResponse;
import fr.su.memorydb.utils.response.SelectResponse;
import fr.su.memorydb.utils.response.ErrorResponse;
import fr.su.memorydb.utils.response.WhereResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Path("select")
public class TableSelection {

    @Inject
    private LocalSelectHandler localSelectHandler;

    @Inject
    private RemoteSelectHandler remoteSelectHandler;

    @Context
    RoutingContext routingContext;

    @Inject
    ToolBox toolBox;

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectColumns(SelectBody selectBody) throws IOException, InterruptedException {
        Instant instant = Instant.now();

        Table table = Database.getInstance().getTables().get(selectBody.table);
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(selectBody.table, "Table '" + selectBody.table + "' not found.").done()).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        WhereBody whereBody = new WhereBody(selectBody.table, selectBody.where);

        // This is used by the forwarding manager to send request to the right endpoint
        toolBox.setUri(toolBox.uri()+"/where");

        // This array is a solution to get the result of the thread operations as it can not initialize variables
        int[][] remoteIndexes = new int[1][];
        Thread thread = new Thread(() -> {
            remoteIndexes[0] = remoteSelectHandler.where(whereBody);
        });
        thread.start();
        int[] localIndexes = localSelectHandler.where(whereBody);
        thread.join();

        // Change that, it's for the tests
        WhereResponse response = new WhereResponse(whereBody.table, null);
        if (localIndexes != null) {
            response.setIndexes(localIndexes);
        } else if (remoteIndexes[0] != null)
            response.setIndexes(remoteIndexes[0]);

        return Response.status(200).entity(response).type(MediaType.APPLICATION_JSON).build();

        /*if(selectBody.requesterIp == null) {
            selectBody.requesterIp = forwardingManager.getLocalIp();
        }
        selectBody.currentIp = forwardingManager.getLocalIp();

        SelectResponse localResponse = localSelectHandler.select(selectBody);
        SelectResponse remoteResponse = remoteSelectHandler.select(selectBody);

        int statusCode = 200;

        if (localResponse == null && remoteResponse == null)
            statusCode = 204;

        List<SelectResponse> list = new ArrayList<>();
        list.add(remoteResponse);
        SelectResponse finaleResponse = localResponse != null ? localResponse.merge(list, selectBody) : remoteResponse != null ? remoteResponse : new SelectResponse(selectBody.table);
        finaleResponse.setStart(instant);
        finaleResponse.aggregate(selectBody);

        return Response.status(statusCode).entity(finaleResponse.done()).type(MediaType.APPLICATION_JSON).build();*/
    }

    @GET
    @Path("where")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWhere(WhereBody whereBody) {
        Table table = Database.getInstance().getTables().get(whereBody.table);
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(whereBody.table, "Table '" + whereBody.table + "' not found.").done()).type(MediaType.APPLICATION_JSON).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        int[] indexes = localSelectHandler.where(whereBody);

        return Response.status(200).entity(new WhereResponse(whereBody.table, indexes)).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("rows")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRows(RowsBody rowsBody) {
        Table table = Database.getInstance().getTables().get(rowsBody.table);
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(rowsBody.table, "Table '" + rowsBody.table + "' not found.").done()).type(MediaType.APPLICATION_JSON).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        // TODO

        return Response.status(200).entity(new RowsResponse()).type(MediaType.APPLICATION_JSON).build();
    }

    public static class SelectBody {

        @JsonProperty
        private String table;

        @JsonProperty
        List<String> columns;

        @JsonProperty
        HashMap<String, SelectOperand> where;

        @JsonProperty
        private Aggregate aggregate;

        private String requesterIp;

        private String currentIp;

        private String groupBy;

        public String getTable() {
            return table;
        }

        public List<String> getColumns() {
            return columns;
        }

        public HashMap<String, SelectOperand> getWhere() {
            return where;
        }

        public Aggregate getAggregate() {
            return aggregate;
        }

        public boolean hasSumAggregate() {
            return aggregate != null && aggregate.sum != null;
        }

        public boolean hasMeanAggregate() {
            return aggregate != null && aggregate.mean != null;
        }

        public boolean hasCountAggregate() {

            return aggregate != null && aggregate.count != null;
        }

        public boolean hasMaxAggregate() {

            return aggregate != null && aggregate.max != null;
        }

        public boolean hasMinAggregate() {

            return aggregate != null && aggregate.min != null;
        }

        public String getGroupBy() {
            return groupBy;
        }

        public boolean hasGroupBy() {

            return groupBy != null && !groupBy.isEmpty();
        }

        public String getRequesterIp() {
            return requesterIp;
        }

        public String getCurrentIp() {
            return currentIp;
        }
    }

    public static class WhereBody {

        @JsonProperty
        private String table;

        @JsonProperty
        HashMap<String, SelectOperand> where;

        public WhereBody() {}

        public WhereBody(String table, HashMap<String, SelectOperand> where) {
            this.table = table;
            this.where = where;
        }

        public String getTable() {
            return table;
        }

        public HashMap<String, SelectOperand> getWhere() {
            return where;
        }

    }

    public static class RowsBody {

        @JsonProperty
        private String table;

        @JsonProperty
        String[] columns;

        @JsonProperty
        int[] indexes;

        public RowsBody() {}

        public RowsBody(String table, String[] columns, int[] indexes) {
            this.table = table;
            this.columns = columns;
            this.indexes = indexes;
        }

        public String getTable() {
            return table;
        }

        public String[] getColumns() {
            return columns;
        }

        public int[] getIndexes() {
            return indexes;
        }

    }

    public static class SelectOperand {

        @JsonProperty
        private Operand operand;

        @JsonProperty
        private String value;

        public Operand getOperand() {
            return operand;
        }

        public String getValue() {
            return value;
        }

    }

    public static class Aggregate {

        private List<String> sum;
        private List<String> mean;
        private List<String> count;
        private List<String> max;
        private List<String> min;

        public List<String> getSum() {
            return sum;
        }

        public List<String> getMean() {
            return mean;
        }

        public List<String> getCount() {
            return count;
        }

        public List<String> getMax() {
            return max;
        }

        public List<String> getMin() {
            return min;
        }
    }

    public enum Operand { EQUALS, BIGGER, LOWER, NOT_EQUALS }
}