package fr.su.memorydb.handlers.insertion;

import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.sql.Array;
import java.util.*;
import java.util.concurrent.*;

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
        int nbMaxThreads = 8;

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(absolutePath), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            Table table = Database.getInstance().getTables().get("test");
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            GroupRecordConverter groupRecordConverter = new GroupRecordConverter(schema);
            List<Thread> threads = new ArrayList<>();

            try (ParquetFileReader r = new ParquetFileReader(conf, new Path(absolutePath), readFooter)) {
                PageReadStore pages = null;

                // Parsing parquet file
                while (null != (pages = r.readNextRowGroup())) {
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, groupRecordConverter);

                    // Number of rows in the current group
                    int nbRows = (int) pages.getRowCount();

                    // HashMap to keep values of this bloc
                    HashMap<Column, Object[]> map = new HashMap<>();
                    for (Column c : table.getColumns()) {
                        if (c.stored())
                            map.put(c, new Object[nbRows]);
                    }

                    // Parsing the group
                    List<Group> groups = new ArrayList<>(nbRows);
                    for (int i = 0; i < nbRows; i++) {
                        groups.add(recordReader.read());
                    }
                    table.rowsCounter += nbRows;

                    // Adding the bloc in the database
                    for (Column c : table.getColumns()) {
                        if (!c.stored())
                            continue;

                        if (threads.size() == nbMaxThreads) {
                            for (Thread thread : threads) {
                                thread.join();
                            }
                            threads.clear();
                        }

                        Thread thread = new Thread(() -> {
                            Object[] rows = map.get(c);
                            if (rows == null)
                                return;

                            for (int i = 0; i < nbRows; i++) {
                                Group g = groups.get(i);

                                try {
                                    Object val;
                                    if (g.getFieldRepetitionCount(c.getName()) != 0)
                                        val = c.getLambda().call(g, c.getName(), 0);
                                    else
                                        val = null;
                                    rows[i] = val;
                                } catch (Exception e) {
                                    rows[i] = null;
                                }
                            }

                            try {
                                c.addRows(rows);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        threads.add(thread);
                        thread.start();
                    }

                    for (Thread thread : threads) {
                        thread.join();
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading parquet file.");
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            System.err.println("Error creating parquet file reader.");
            e.printStackTrace();
        }
    }

}