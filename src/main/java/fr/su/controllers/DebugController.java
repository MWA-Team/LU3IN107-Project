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
            sb.append("Table : " + table.getName() + " : " + table.getColumnsNames().values().stream().map(all -> all.getName()).collect(Collectors.joining(", ")));

            System.out.println("First line : ");

//            System.out.println("Types :");
//            for (Column column : table.getColumns().values())
//                System.out.println(column.getName() + " " + column.storedHere());

            for(String key : table.getColumnsNames().keySet()) {
                for (Object o : table.getColumnsNames().get(key).getValues()) {
                    System.out.println("Clef : " + key + " | Value : " + o);
                    break;
                }
            }

            if (table.getColumnsNames().size() >= 1) {
                System.out.println("First column");
                for (String key : table.getColumnsNames().keySet()) {
                    for (Object o : table.getColumnsNames().get(key).getValues()) {
                        System.out.println(o);
                    }
                    break;
                }
            }
        }

        return sb.toString();
    }
}
