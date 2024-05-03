package fr.su.memorydb.handlers.insertion;

import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
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

            try (ParquetFileReader r = new ParquetFileReader(conf, new Path(absolutePath), readFooter)) {
                PageReadStore pages = null;
                int count = 0;
                while (null != (pages = r.readNextRowGroup())) {
                    long rows = pages.getRowCount();
                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < rows; i++) {
                        Group g = recordReader.read();
                        addGroup(g, count++);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading parquet file.");
            e.printStackTrace();
        }
    }

    private static void addGroup(Group g, int index) {
        for (Column c : Database.getInstance().getTables().get("test").getColumns()) {
            if (!c.stored())
                continue;
            try {
                if (g.getFieldRepetitionCount(c.getName()) != 0)
                    c.addRowGroup(g, c.getName(), index);
                else
                    c.addRowValue(null, index);
            } catch (Exception e) {
                c.addRowValue(null, index);
            }
        }
    }

    private List<ColumnDescriptor> getTableColumns(String absolutePath) throws IOException {
        ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
        return readFooter == null ? null : readFooter.getFileMetaData().getSchema().getColumns();
    }

}