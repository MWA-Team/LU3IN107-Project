package fr.su.controllers;

import fr.su.database.Database;
import fr.su.database.Table;
import jakarta.ws.rs.*;

import javax.xml.crypto.Data;
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

            System.out.println("First line : ");
            for(String keys : table.getColumns().keySet()) {

                System.out.println("Clef : " + keys + " | Value : " + table.getColumns().get(keys).getValues().get(0));

            }

            System.out.println("First column");
            for(int i = 0; i < table.getColumns().get("area").getValues().size(); i++) {

                System.out.println(table.getColumns().get("area").getValues().get(i));
            }

        }

        return sb.toString();
    }
}
