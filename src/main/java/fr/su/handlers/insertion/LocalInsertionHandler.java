package fr.su.handlers.insertion;

import fr.su.database.Database;
import fr.su.utils.exceptions.TableColumnSizeException;
import fr.su.utils.exceptions.WrongTableFormatException;
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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LocalInsertionHandler implements InsertionHandler {

    private final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("insertion.parquet");

    @Override
    public int insert(InputStream inputStream) throws WrongTableFormatException, TableColumnSizeException {

        try (OutputStream outputStream = new FileOutputStream("insertion.parquet")) {
            int bytesRead;
            byte[] buffer = new byte[1024];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> columns = getTableSchema();

        if(Database.getInstance().getTables().size() != columns.size()) { //on regarde si le nombre de colonne dans le fichier correspond bien aunombre de colonne dans notre BD

            throw new TableColumnSizeException(Database.getInstance().getTables().size(), columns.size());
        }

        //handlerFile();
        System.out.println(getTableSchema());// nous donne le format actual de la table

        //On va vérifier si la structure du .parquet correspond à la structure de notre table
        //On va récupérer une partie des données, les autres seront récupérées par les autres serveurs. (Règle de récupération à définir)

        return 200;

    }

    private void handlerFile() {

        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

            PageReadStore pages = null;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    System.out.println("Number of rows: " + rows);

                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int i = 0; i < rows; i++) {
                        final Group g = recordReader.read();
                        printGroup(g);
                        System.out.println("TEST");
                        System.out.println(g);
                        System.out.println("--");
                    }
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
    }

    private List<String> getTableSchema() {
        List<String> columnList = new ArrayList<>();

        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();

            List<Type> fields = schema.getFields();
            for (Type field : fields) {
                columnList.add(field.getName());
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return columnList;
    }

    private static void printGroup(Group g) {

        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(fieldName + " " + g.getValueToString(field, index));
                }
            }
        }

    }
}
