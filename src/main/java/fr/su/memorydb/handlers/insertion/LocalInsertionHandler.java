package fr.su.memorydb.handlers.insertion;

import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.exceptions.WrongTableFormatException;
import jakarta.inject.Inject;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Singleton
public class LocalInsertionHandler implements InsertionHandler {

    @Inject
    ToolBox toolBox;

    int nbMaxThreads = 8;

    @Override
    public int insert(File file, String table) throws WrongTableFormatException {
        String absolutePath = file.getAbsolutePath();
        handlerFile(new Path(absolutePath), table);
        return 200;
    }

    private void handlerFile(Path absolutePath, String tableName) {
        try {
            Configuration conf = new Configuration();
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, absolutePath, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            Table table = Database.getInstance().getTables().get(tableName);
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            GroupRecordConverter groupRecordConverter = new GroupRecordConverter(schema);

            int blocsSize;
            if (toolBox.blocsSize() <= 0) {
                if (ToolBox.realBlocsSize() <= 0) {
                    ToolBox.setRealBlocsSize(1048576);
                }
                for (Column column : table.getColumns()) {
                    column.setBlocsSize(ToolBox.realBlocsSize());
                }
            } else if (ToolBox.realBlocsSize() <= 0)
                ToolBox.setRealBlocsSize(toolBox.blocsSize());

            try (ParquetFileReader r = new ParquetFileReader(conf, absolutePath, readFooter)) {
                PageReadStore pages = null;

                int max = ToolBox.realBlocsSize();
                for (Column column : table.getColumns()) {
                    if (!column.stored())
                        continue;
                    if (!column.isLastBlocIsFull()) {
                        int tmp = (ToolBox.realBlocsSize() - column.getLastBlocsSize()) + max;
                        if (max < tmp)
                            max = tmp;
                        break;
                    }
                }
                List<Group> groups = new ArrayList<>(max);

                // HashMap to keep values of this bloc
                HashMap<Column, Object[]> map = new HashMap<>();
                initMap(map, table, max);

                // Parsing parquet file
                while (null != (pages = r.readNextRowGroup())) {
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, groupRecordConverter);

                    // Number of rows in the current group
                    int nbRows = (int) pages.getRowCount();

                    // Parsing the groups
                    for (int i = 0; i < nbRows; i++) {
                        groups.add(recordReader.read());
                        table.rowsCounter++;
                        // Adding the bloc in the database
                        if (groups.size() == max) {
                            addBloc(groups, table, map);
                            if (max != ToolBox.realBlocsSize()) {
                                max = ToolBox.realBlocsSize();
                                groups = new ArrayList<>(max);
                            }
                            groups.clear();
                            initMap(map, table, max);
                        }
                    }
                }
                if (!groups.isEmpty()) {
                    addBloc(groups, table, map);
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

    public void addBloc(List<Group> groups, Table table, HashMap<Column, Object[]> map) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();

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

                HashSet<Object> values = c.indexingEnabled() ? new HashSet<>() : null;

                for (int i = 0; i < groups.size(); i++) {
                    Group g = groups.get(i);

                    try {
                        Object val;
                        if (g.getFieldRepetitionCount(c.getName()) != 0)
                            val = c.getLambda().call(g, c.getName(), 0);
                        else
                            val = null;
                        rows[i] = val;
                        if (values != null)
                            values.add(val);
                    } catch (Exception e) {
                        rows[i] = null;
                    }
                }

                if (values != null && values.size() / ((float)table.rowsCounter) < toolBox.indexingThreshold())
                    c.disableIndexing();

                try {
                    c.addRows(rows, groups.size());
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

    private void initMap(HashMap<Column, Object[]> map, Table table, int nbRows) {
        for (Column c : table.getColumns()) {
            if (c.stored())
                map.put(c, new Object[nbRows]);
        }
    }

}