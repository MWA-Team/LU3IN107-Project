package fr.su.handlers.insertion;

import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
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

import java.io.File;
import java.io.IOException;
import java.util.List;

@Singleton
public class LocalInsertionHandler implements InsertionHandler {

    @Override
    public int insert(File file) throws WrongTableFormatException {
        String absolutePath = file.getAbsolutePath();
        handlerFile(absolutePath);
        return 200;
    }

    private void handlerFile(String absolutePath) {
        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, new Path(absolutePath), readFooter);

            PageReadStore pages = null;
            int count = 0;
            int page = 0;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    long rows = pages.getRowCount();
                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < rows; i++) {
                        Group g = (Group) recordReader.read();
                        addGroup(g, count++);
                    }
                    page++;
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.err.println("Error reading parquet file.");
            e.printStackTrace();
        }
    }

    private static void addGroup(Group g, int index) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            Column column = Database.getInstance().getTables().get("test").getColumns().get(fieldName);

            if (column != null) {
                if (!column.stored())
                    continue;
                if (fieldType.isPrimitive()) {
                    try {
                        if (g.getFieldRepetitionCount(field) != 0)
                            column.addRowGroup(g, field, index);
                        else
                            column.addRowValue(null, index);
                    } catch (Exception e) {
                        column.addRowValue(null, index);
                    }
                } else {
                    Group nestedGroup = g.getGroup(field, 0);
                    addGroup(nestedGroup, index);
                }
            }
        }
    }

    private List<ColumnDescriptor> getTableColumns(String absolutePath) throws IOException {
        ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
        return readFooter == null ? null : readFooter.getFileMetaData().getSchema().getColumns();
    }

}