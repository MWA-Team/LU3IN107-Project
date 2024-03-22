package fr.su.controllers;

import fr.su.database.Column;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestForm;

import java.util.List;

@Path("/table")
public class TableCreation {

    @POST
    @Produces({MediaType.TEXT_PLAIN})
    public String table(TableBody tableBody) {
        return "Hello RESTEasy";
    }

    public class TableBody {

        @RestForm
        private String tableName;

        @RestForm
        private List<String> columns;

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
