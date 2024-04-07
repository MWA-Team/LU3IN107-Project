package fr.su.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.su.handlers.table.LocalTableHandler;
import fr.su.handlers.table.RemoteTableHandler;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

@Path("table")
public class TableController {

    private final LocalTableHandler localTableHandler = new LocalTableHandler();
    private final RemoteTableHandler remoteTableHandler = new RemoteTableHandler();

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces({MediaType.APPLICATION_JSON})
    public TableBody table(TableBody tableBody) {

        localTableHandler.createTable(tableBody);
        //TODO Operations
        remoteTableHandler.createTable(tableBody);

        return tableBody;
    }

    public static class TableBody {

        @JsonProperty
        private String tableName;

        @JsonProperty
        private List<TableParameter> columns;

        public String getTableName() { return tableName; }

        public List<TableParameter> getColumns() { return columns; }

    }

    public static class TableParameter {

        private String name;
        private String type;

        public String getName() { return this.name; }
    }


    /**
     * Exemple de body
     * POST
     *
     * {
     *     "tableName":"test",
     *     "columns": [
     *
     *          {
     *              "columnName": "nom",
     *              "type": "STRING"
     *
     *          },
     *
     *          {
     *              "columnName": "prenom",
     *              "type": "STRING"
     *          },
     *
     *          {
     *              "columnName": "age",
     *              "type": "INTEGER"
     *          }
     *     ]
     *
     *
     *
     * }
     */

}   
