package fr.su.controllers;

import fr.su.handlers.insertion.LocalInsertionHandler;
import jakarta.ws.rs.*;
import java.util.Map;

@Path("/select")
public class TableSelection {
    private final LocalInsertionHandler localInsertionHandler = new LocalInsertionHandler();

    @GET
    public void executeSelect(String query) {
        if (query.trim().equalsIgnoreCase("select * from table")) {
            Map<String, Map<Integer, Object>> parquetData = localInsertionHandler.parseParquet();

            parquetData.forEach((column, values) -> {
                System.out.println("Column: " + column);
                values.forEach((row, value) -> {
                    System.out.println("Row " + row + ": " + value);
                });
            });
        } else {
            System.out.println("Query is false");
        }
    }
}
