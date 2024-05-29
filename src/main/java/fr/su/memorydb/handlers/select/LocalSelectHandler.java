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

            switch (operand) {
                case EQUALS:
                    int[] indexesEquals = column.get(converter.call((String) compare));
                    if (indexesEquals != null)
                        evaluatedIndexes.add(indexesEquals);
                    break;
                case BIGGER:
                    int[] indexesBigger = column.getBigger(converter.call((String) compare));
                    if (indexesBigger != null)
                        evaluatedIndexes.add(indexesBigger);
                    break;
                case LOWER:
                    int[] indexesLower = column.getLower(converter.call((String) compare));
                    if (indexesLower != null)
                        evaluatedIndexes.add(indexesLower);
                    break;
                case NOT_EQUALS:
                    int[] indexesNotEquals = column.getNotEquals(converter.call((String) compare));
                    if (indexesNotEquals != null)
                        evaluatedIndexes.add(indexesNotEquals);
                    break;
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
    public List<HashMap<String, Object>> select(TableSelection.SelectBody selectBody, int[] indexes) throws IOException {
        Table table = Database.getInstance().getTables().get(selectBody.getTable());
        boolean worked = false;
        List<HashMap<String, Object>> rows = new ArrayList<>(indexes != null ? indexes.length : table.rowsCounter);

        // Parsing which row to select based on the indexes
        for(Column column : Database.getInstance().getTables().get(selectBody.getTable()).getColumns()) {
            if (column.stored()) {
                if (selectBody.getColumns().contains(column.getName()) || (selectBody.getGroupBy() != null && selectBody.getGroupBy().contains(column.getName())) || (selectBody.getAggregate() != null && selectBody.getAggregate().isAggregated(column.getName()))) {
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
                            int tmp = selectIndex(column, row, values, bloc, i);
                            if (tmp != bloc) {
                                values = column.getValues(tmp);
                                bloc = tmp;
                            }
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
                            int tmp = selectIndex(column, row, values, bloc, index);
                            if (tmp != bloc) {
                                values = column.getValues(tmp);
                                bloc = tmp;
                            }
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
