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
        Instant start = Instant.now();

        Table table = Database.getInstance().getTables().get(selectBody.table);
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(selectBody.table, "Table '" + selectBody.table + "' not found.").setStart(start).done()).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        /**
         *  GETTING INDEXES FILTERED WITH WHERE CONDITION
         */

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

        // Merging the indexes
        List<int[]> tmpIndexes = new ArrayList<>(2);
        tmpIndexes.add(localIndexes);
        tmpIndexes.add(remoteIndexes[0]);
        int[] indexes = WhereResponse.mergeIndexes(tmpIndexes);

        /**
         * GETTING THE ROWS FILTERED USING THE PREVIOUSLY GOTTEN INDEXES
         */

        // This is used by the forwarding manager to send request to the right endpoint
        toolBox.setUri(toolBox.uri()+"/rows");

        // This array is a solution to get the result of the thread operations as it can not initialize variables
        List<HashMap<String, Object>>[] remoteRows = new List[1];
        thread = new Thread(() -> {
            try {
                remoteRows[0] = remoteSelectHandler.select(selectBody, indexes);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        List<HashMap<String, Object>> localRows = localSelectHandler.select(selectBody, indexes);
        thread.join();

        // Merging the rows
        List<List<HashMap<String, Object>>> tmpRows = new ArrayList<>(2);
        tmpRows.add(localRows);
        tmpRows.add(remoteRows[0]);

        List<HashMap<String, Object>> rows = RowsResponse.mergeRows(tmpRows, selectBody);

        SelectResponse response = new SelectResponse(selectBody.table, rows);

        return Response.status(200).entity(response.details("Select operation was successful !").setStart(start).done()).type(MediaType.APPLICATION_JSON).build();
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
    public Response getRows(RowsBody rowsBody) throws IOException {
        Table table = Database.getInstance().getTables().get(rowsBody.getTable());
        if (table == null) {
            return Response.status(404).entity(new ErrorResponse(rowsBody.getTable(), "Table '" + rowsBody.getTable() + "' not found.").done()).type(MediaType.APPLICATION_JSON).build();
        }

        ToolBox.Context context = new ToolBox.Context(routingContext.request().uri(), routingContext.queryParams().get("server_id"), routingContext.request().headers().get("Server-Signature"));
        ToolBox.setContext(context);

        List<HashMap<String, Object>> rows = localSelectHandler.select(rowsBody.getSelectBody(), rowsBody.getIndexes());

        return Response.status(200).entity(new RowsResponse(rowsBody.getTable(), rows)).type(MediaType.APPLICATION_JSON).build();
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

        private List<String> groupBy;

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

        public List<String> getGroupBy() {
            return groupBy;
        }

        public boolean hasGroupBy() {
            return groupBy != null && !groupBy.isEmpty();
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
        int[] indexes;

        @JsonProperty
        SelectBody selectBody;

        public RowsBody() {}

        public RowsBody(SelectBody selectBody, int[] indexes) {
            this.selectBody = selectBody;
            this.indexes = indexes;
        }

        public String getTable() {
            return selectBody.getTable();
        }

        public int[] getIndexes() {
            return indexes;
        }

        public SelectBody getSelectBody() {return selectBody;}

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

        public boolean isAggregated(String column) {
            return (sum != null && sum.contains(column)) || (count != null && count.contains(column)) || (mean != null && mean.contains(column));
        }

        public void sum(List<HashMap<String, Object>> rows, String column, List<Integer> indexes, HashMap<String, Object> output) {
            if (rows == null || rows.isEmpty())
                return;

            if (indexes != null) {
                String name = "sum-" + column;
                double total = 0;
                for (Integer index : indexes) {
                    Object value = rows.get(index).get(column);
                    if (value == null)
                        continue;
                    if (!(value instanceof Number))
                        throw new RuntimeException("You're using the sum aggregate on a non-number column !");
                    total += ((Number) value).doubleValue();
                }
                output.put(name, total);
            } else {
                String name = "sum-" + column;
                double total = 0;
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(column);
                    if (value == null)
                        continue;
                    if (!(value instanceof Number))
                        throw new RuntimeException("You're using the sum aggregate on a non-number column !");
                    total += ((Number) value).doubleValue();
                }
                output.put(name, total);
            }
        }

        public void mean(List<HashMap<String, Object>> rows, String column, List<Integer> indexes, HashMap<String, Object> output) {
            if (rows == null || rows.isEmpty())
                return;

            if (indexes != null) {
                String name = "mean-" + column;
                double total = 0;
                for (Integer index : indexes) {
                    Object value = rows.get(index).get(column);
                    if (value == null)
                        continue;
                    if (!(value instanceof Number))
                        throw new RuntimeException("You're using the mean aggregate on a non-number column !");
                    total += ((Number) value).doubleValue();
                }
                output.put(name, total / indexes.size());
            } else {
                String name = "mean-" + column;
                double total = 0;
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(column);
                    if (value == null)
                        continue;
                    if (!(value instanceof Number))
                        throw new RuntimeException("You're using the mean aggregate on a non-number column !");
                    total += ((Number) value).doubleValue();
                }
                output.put(name, total / rows.size());
            }
        }

        public void count(List<HashMap<String, Object>> rows, String column, List<Integer> indexes, HashMap<String, Object> output) {
            if (rows == null || rows.isEmpty())
                return;

            if (indexes != null) {
                String name = "count-" + column;
                output.put(name, indexes.size());
            } else {
                String name = "count-" + column;
                output.put(name, rows.size());
            }
        }

    }

    public enum Operand { EQUALS, BIGGER, LOWER, NOT_EQUALS }
}
