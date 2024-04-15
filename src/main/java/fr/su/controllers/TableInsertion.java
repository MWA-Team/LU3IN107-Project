package fr.su.controllers;

import fr.su.handlers.ForwardingManager;
import fr.su.handlers.insertion.LocalInsertionHandler;
import fr.su.handlers.insertion.RemoteInsertionHandler;
import fr.su.handlers.table.LocalTableHandler;
import fr.su.handlers.table.RemoteTableHandler;
import fr.su.utils.exceptions.TableColumnSizeException;
import fr.su.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Path("/insert")
public class TableInsertion {

    private final LocalInsertionHandler localInsertionHandler = new LocalInsertionHandler();
    private final RemoteInsertionHandler remoteInsertionHandler = new RemoteInsertionHandler();

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Object insertion(InputStream inputStream) throws IOException, WrongTableFormatException, TableColumnSizeException {

        int resLocal = localInsertionHandler.insert(inputStream);
        int resRemote = remoteInsertionHandler.insert(inputStream);

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

        //Test, ajouter un message et implémenter un système d'annulation si un des forward a échoué
        return resLocal == 200 && resRemote == 200 ? "OK" : "NON OK";
    }

}
