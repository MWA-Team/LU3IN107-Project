package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.database.Table;
import fr.su.memorydb.utils.ToolBox;
import fr.su.memorydb.utils.response.RowsResponse;
import fr.su.memorydb.utils.response.SelectResponse;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Inject
    ToolBox toolBox;

    @Override
    public int[] where(TableSelection.WhereBody whereBody) {
        Table table = Database.getInstance().getTables().get(whereBody.getTable());
        List<int[]> evaluatedIndexes = new LinkedList<>();

        for(Column column : table.getColumns()) {
            if (!column.stored() || !whereBody.getWhere().containsKey(column.getName()))
                continue;

            LambdaTypeConverter converter = column.getConverter();
            TableSelection.SelectOperand condition = whereBody.getWhere().get(column.getName());
            TableSelection.Operand operand = condition.getOperand();
            Object compare = condition.getValue();

            if (operand.equals(TableSelection.Operand.EQUALS)) {
                try {
                    int[] indexes = column.get(converter.call((String) compare));
                    evaluatedIndexes.add(indexes);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // No where on this server
        if (evaluatedIndexes.isEmpty())
            return null;

        int length = 0;
        for (int[] indexes : evaluatedIndexes) {
            if (indexes != null)
                length += indexes.length;
        }

        if (length == 0)
            return new int[0];

        int[] result = new int[length];
        int lastIndex = 0;
        for (int[] indexes : evaluatedIndexes) {
            if (indexes == null)
                continue;

            for (int index : indexes)
                result[lastIndex++] = index;
        }

        return result;
    }

    @Override
    public List<HashMap<String, Object>> select(TableSelection.SelectBody selectBody, int[] indexes) throws IOException, InterruptedException {
        Table table = Database.getInstance().getTables().get(selectBody.getTable());
        boolean worked = false;
        List<HashMap<String, Object>> rows = new ArrayList<>(indexes != null ? indexes.length : table.rowsCounter);

        // Parsing which row to select based on the indexes
        for(Column column : Database.getInstance().getTables().get(selectBody.getTable()).getColumns()) {
            if (column.stored()) {
                if (selectBody.getColumns().contains(column.getName())) {
                    worked = true;
                    int bloc = 0;
                    Object[] values = column.getValues(bloc);
                    HashMap<String, Object> row;
                    if (indexes == null) {
                        for (int i = 0; i < table.rowsCounter; i++) {
                            if (rows.size() <= i) {
                                row  = new HashMap<>();
                                rows.add(row);
                            } else {
                                row = rows.get(i);
                            }
                            bloc = selectIndex(column, row, values, bloc, i);
                        }
                    } else {
                        int i = 0;
                        for (int index : indexes) {
                            if (rows.size() <= i) {
                                row  = new HashMap<>();
                                rows.add(row);
                            } else {
                                row = rows.get(i);
                            }
                            bloc = selectIndex(column, row, values, bloc, index);
                            i++;
                        }
                    }

                }
            }
        }

        // If there is nothing to do on this server, return null
        if (!worked)
            return null;

        return rows;
          
        /*//Managing group by : decrementing in negative numbers
        if(selectBody.hasGroupBy()) {

            int index = -1;
            String column = selectBody.getGroupBy();

            Column clm = Database.getInstance().getTables().get(selectBody.getTable()).getColumn(column);
            if (clm.stored()) {

                List<HashMap<Object, Object>> rows = clm.getRows();

                List<Object> diffObjects = new ArrayList<>();

                for(HashMap<Object, Object> hash : rows) {

                    for(Object keys : hash.entrySet()) {

                        Map.Entry<Object, Object> res = (Map.Entry<Object, Object>) keys;

                        if(!diffObjects.contains(((Map.Entry<?, ?>) keys).getKey())) {
                            diffObjects.add(((Map.Entry<?, ?>) keys).getKey());
                        }
                    }
                }

                for(Object obj : diffObjects) { //Passing cross all groups

                    HashMap base = new HashMap();

                    for (Column column1 : toShow) {
                        base.put(column1.getName(), (column1.getName().equals(column)) ? obj : Double.NaN); //we can't access specific index here, so we will access to it later in SelectResponse
                    }

                    selectResponse.add(index, base);
                    index -= 1;
                }

            }

        }
        return selectResponse;*/
    }

    private int selectIndex(Column column, HashMap<String, Object> row, Object[] values, int bloc, int index) throws IOException {
        int retval = bloc;

        if (index / ToolBox.realBlocsSize() != bloc) {
            retval = index / ToolBox.realBlocsSize();
            values = column.getValues(retval);
        }

        row.put(column.getName(), values[index - retval * ToolBox.realBlocsSize()]);

        return retval;
    }

}
