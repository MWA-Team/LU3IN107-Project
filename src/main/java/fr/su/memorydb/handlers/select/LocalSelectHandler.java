package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import jakarta.inject.Singleton;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.*;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) throws IOException {
        HashSet<Column> toShow = new HashSet<>();
        HashSet<Column> toEvaluate = new HashSet<>();
        LinkedList<int[]> evaluatedIndexes = new LinkedList<>();
        SelectResponse selectResponse = new SelectResponse();

        // Parsing which column to show and which column to evaluate (where clause)
        for(Column column : Database.getInstance().getTables().get(selectBody.getTable()).getColumns()) {
            if (column.stored()) {
                if(selectBody.getWhere().containsKey(column.getName()))
                    toEvaluate.add(column);
                if (selectBody.getColumns().contains(column.getName()))
                    toShow.add(column);
            }
        }

        // If there is nothing to do on this server, return null
        if (toShow.isEmpty() && toEvaluate.isEmpty())
            return null;

        // Getting all indexes that match their condition in the related columns
        for (Column column : toEvaluate) {
            LambdaTypeConverter converter = column.getConverter();
            Object compare = selectBody.getWhere().get(column.getName()).getValue();
            TableSelection.Operand operand = selectBody.getWhere().get(column.getName()).getOperand();

            if (operand.equals(TableSelection.Operand.EQUALS)) {
                int[] indexes = column.get(converter.call((String) compare));
                if (indexes != null)
                    evaluatedIndexes.add(indexes);
            }
        }

        // If there was a filter on this server use evaluatedIndex, else return all indexes on all selected columns
        if (evaluatedIndexes.isEmpty() && !toEvaluate.isEmpty())
            return new SelectResponse();

        int[] indexes = null;
        if (!evaluatedIndexes.isEmpty())
            indexes = evaluatedIndexes.pop();
        else {
            for (Column column : toShow) {
                indexes = column.getAllIndexes();
                break;
            }
        }

        HashMap<Column, Object[]> columnsValues = new HashMap<>();
        for (Column column : toShow) {
            columnsValues.put(column, column.getValuesAsArray());
        }
        // Building response
        for (Integer index : indexes) {
            boolean pass = false;

            for (int[] indexSet : evaluatedIndexes) {
                int found = Arrays.binarySearch(indexSet, index);
                if (found < 0) {
                    pass = true;
                    break;
                }
            }

            if (!evaluatedIndexes.isEmpty() && pass)
                continue;

            HashMap<String, Object> row = new HashMap<>();
            for (Column column : toShow) {
                row.put(column.getName(), columnsValues.get(column)[index]);
            }
            selectResponse.add(index, row);
        }
        return selectResponse;
    }

}
