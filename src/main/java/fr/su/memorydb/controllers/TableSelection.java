package fr.su.memorydb.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.handlers.ForwardingManager;
import fr.su.memorydb.handlers.select.LocalSelectHandler;
import fr.su.memorydb.handlers.select.RemoteSelectHandler;
import fr.su.memorydb.utils.response.SelectResponse;
import fr.su.memorydb.utils.response.ErrorResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Path("select")
public class TableSelection{

    @Inject
    private LocalSelectHandler localSelectHandler;

    @Inject
    private RemoteSelectHandler remoteSelectHandler;

    @Inject
    private ForwardingManager forwardingManager;

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectColumns(SelectBody selectBody) throws IOException, InterruptedException {

        Instant instant = Instant.now();

        Database database = Database.getInstance();
        Table table = database.getTables().get(selectBody.table);
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(selectBody.table, "Table '" + selectBody.table + "' not found.").done()).build();
        }

        if(selectBody.requesterIp == null) {
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

        return Response.status(statusCode).entity(finaleResponse.done()).type(MediaType.APPLICATION_JSON).build();
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