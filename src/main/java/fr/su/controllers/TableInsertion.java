package fr.su.controllers;

import fr.su.handlers.insertion.LocalInsertionHandler;
import fr.su.handlers.insertion.RemoteInsertionHandler;
import fr.su.utils.exceptions.TableColumnSizeException;
import fr.su.utils.exceptions.WrongTableFormatException;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Path("/insert")
public class TableInsertion {

    private final LocalInsertionHandler localInsertionHandler = new LocalInsertionHandler();
    private final RemoteInsertionHandler remoteInsertionHandler = new RemoteInsertionHandler();

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Object insertion(InputStream inputStream) {
        try {
            int responseCode = localInsertionHandler.insert(inputStream);

            if (responseCode == 200) {
                Map<String, Map<Integer, Object>> parquetData = localInsertionHandler.parseParquet();
                parquetData.forEach((column, values) -> {
                    System.out.println("Column: " + column);
                    values.forEach((row, value) -> {
                        System.out.println("Row " + row + ": " + value);
                    });
                });
                return "OK";
            } else {
                return "Error during insertion";
            }
        } catch (WrongTableFormatException e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }



   /*           verifier si c'est parquet avec PAR1
                save le parquet sous forme de fichier
                valider le shema du parquet avec le shema de notre db
            }*/
        /**
         *
         * EXEMPLE
         *
         * ApacheReaderFile f;
         *
         * List<String> strct = f.getStructure(); //Toutes les colonnes
         * HashMap<String, List<String>> content;
         *
         * for(String column : strct)
         *
         *      content.put(column, new ArrayList<>()); //création d'une colonne vide
         *
         *      content.get(column).addAll(f.getColumn(column));
         *
         *
         */
        //int resRemote = remoteInsertionHandler.insert(inputStream);
        //Test, ajouter un message et implémenter un système d'annulation si un des forward a échoué
    }

}
