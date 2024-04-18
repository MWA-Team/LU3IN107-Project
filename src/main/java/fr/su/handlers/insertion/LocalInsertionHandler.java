package fr.su.handlers.insertion;

import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;
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
import java.nio.file.Files;
import java.util.*;

public class LocalInsertionHandler implements InsertionHandler {

    //private final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("insertion.parquet");

    private Map<String, Map<Integer, Object>> parquetData;


    //public static boolean isParquetFile à faire

    @Override
    public int insert(InputStream inputStream) throws WrongTableFormatException {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("insertion", ".parquet");
            try (OutputStream outputStream = new FileOutputStream(tempFile)) {
                int bytesRead;
                byte[] buffer = new byte[1024];
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
            String absolutePath = tempFile.getAbsolutePath();
            //if(!validateSchema(absolutePath))
                //throw  new WrongTableFormatException();

            handlerFile(absolutePath);
            parquetData = parseParquet();
            System.out.println(getTableSchema(absolutePath));
            return 200;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.delete(tempFile.toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Map<String, Map<Integer, Object>> parseParquet() {
        Map<String, Map<Integer, Object>> dataMap = new HashMap<>();

        try {
            if (parquetData != null) {
                dataMap.putAll(parquetData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private void handlerFile(String absolutePath) {
        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new org.apache.hadoop.fs.Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, new org.apache.hadoop.fs.Path(absolutePath), readFooter);

            PageReadStore pages = null;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    System.out.println("Number of rows: " + rows);

                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int i = 0; i < rows; i++) {
                        final Group g = recordReader.read();
                        printGroup(g, "");
                        System.out.println("-----");
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

    private List<String> getTableSchema(String absolutePath) {
        List<String> columnList = new ArrayList<>();

        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new org.apache.hadoop.fs.Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
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


    private static void printGroup(Group g, String prefix) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(prefix + fieldName + ": " + g.getValueToString(field, index));
                } else {
                    Group nestedGroup = g.getGroup(field, index);
                    printGroup(nestedGroup, prefix + fieldName + ".");
                }
            }
        }
    }

    public boolean validateSchema(String absolutePath) {
        List<String> parquetSchema = getTableSchema(absolutePath);
        Map<String, Table> databaseTables = Database.getInstance().getTables();

        if (parquetSchema.size() != databaseTables.size()) {
            return false;
        }

        for (String tableName : parquetSchema) {
            if (!databaseTables.containsKey(tableName)) {
                return false;
            }

            Table table = databaseTables.get(tableName);
            Map<UUID, Column> tableColumns = table.getColumns();
            List<String> parquetTableColumns = getTableColumns(absolutePath, tableName);

            if (parquetTableColumns.size() != tableColumns.size()) {
                return false;
            }

            for (String columnName : parquetTableColumns) {
                if (!tableColumns.containsKey(columnName)) {
                    return false;
                }
            }
        }
        return true;
    }

    private List<String> getTableColumns(String absolutePath, String tableName) {
        List<String> tableColumns = new ArrayList<>();

        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new org.apache.hadoop.fs.Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();

            List<Type> fields = schema.getFields();
            for (Type field : fields) {
                if (field.getName().equals(tableName)) {
                    List<Type> columns = field.asGroupType().getFields();
                    for (Type column : columns) {
                        tableColumns.add(column.getName());
                    }
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("Error parsing parquet");
            e.printStackTrace();
        }

        return tableColumns;
    }

}
