package fr.su.controllers;

import fr.su.database.Database;
import fr.su.database.Table;
import jakarta.ws.rs.*;

import java.util.stream.Collectors;

@Path("debug")
public class DebugController {


    @Path("database")
    @GET
    public String database() {

        Database database = Database.getInstance();

        StringBuilder sb = new StringBuilder();

        sb.append("Database tables : " + database.getTables().size());
        sb.append("\n");

        for(Table table : database.getTables().values()) {

            sb.append("Table : " + table.getName() + " : " + table.getColumns().values().stream().map(all -> all.getName()).collect(Collectors.joining(", ")));
        }

        return sb.toString();
    }
}
