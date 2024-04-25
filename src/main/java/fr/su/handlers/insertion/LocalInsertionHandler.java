package fr.su.handlers.insertion;

import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.database.Table;
import fr.su.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class LocalInsertionHandler implements InsertionHandler {

    @Override
    public int insert(File file) throws WrongTableFormatException {
        /*try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        }*/
        String absolutePath = file.getAbsolutePath();
        List<ColumnDescriptor> columns = null;
        try {
            columns = getTableColumns(absolutePath);
        } catch (IOException e) {
            return 403;
        }

        handlerFile(absolutePath, columns);
        return 200;
    }

    public Map<String, Map<Integer, Object>> parseParquet() {
        Map<String, Map<Integer, Object>> parquetData = new HashMap<>();

        try {
            if (parquetData != null) {
                parquetData.putAll(parquetData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parquetData;
    }

    private void handlerFile(String absolutePath, List<ColumnDescriptor> columns) {
        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, new Path(absolutePath), readFooter);

            PageReadStore pages = null;
            long count = 0;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();

                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < rows; i++) {
                        Group g = (Group) recordReader.read();
                        addGroup(g, count++);
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

    private static void addGroup(Group g, long index) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            Column column = Database.getInstance().getTables().get("test").getColumns().get(fieldName);
            if (column != null) {
                if (!column.stored())
                    continue;
                if (fieldType.isPrimitive()) {
                    column.addValue(index, g.getValueToString(field, 0));
                } else {
                    Group nestedGroup = g.getGroup(field, 0);
                    addGroup(nestedGroup, index);
                }
            }
        }
    }

    public boolean validateSchema(String absolutePath) throws IOException {
        List<ColumnDescriptor> parquetSchema = getTableColumns(absolutePath);
        Map<String, Table> databaseTables = Database.getInstance().getTables();

        if (parquetSchema.size() != databaseTables.size()) {
            return false;
        }

        for (ColumnDescriptor tableName : parquetSchema) {
            if (!databaseTables.containsKey(tableName)) {
                return false;
            }

            Table table = databaseTables.get(tableName);
            Map<String, Column> tableColumns = table.getColumns();
            List<ColumnDescriptor> parquetTableColumns = getTableColumns(absolutePath);

            if (parquetTableColumns.size() != tableColumns.size()) {
                return false;
            }

            for (ColumnDescriptor columnName : parquetTableColumns) {
                if (!tableColumns.containsKey(columnName)) {
                    return false;
                }
            }
        }
        return true;
    }

    private List<ColumnDescriptor> getTableColumns(String absolutePath) throws IOException {
        ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
        return readFooter == null ? null : readFooter.getFileMetaData().getSchema().getColumns();
    }

}
