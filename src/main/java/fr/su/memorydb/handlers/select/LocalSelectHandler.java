package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import jakarta.inject.Singleton;

import java.util.*;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) {
        HashSet<Column> toShow = new HashSet<>();
        HashSet<Column> toEvaluate = new HashSet<>();
        LinkedList<HashSet<Integer>> evaluatedIndexes = new LinkedList<>();
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
                HashSet<Integer> tmp = (HashSet<Integer>) column.getRows().get(converter.call((String) compare));
                if (tmp != null)
                    evaluatedIndexes.add(tmp);
            }
        }

        // If there was a filter on this server use evaluatedIndex, else return all indexes on all selected columns
        boolean filterOccurred = !toEvaluate.isEmpty();
        if (evaluatedIndexes.isEmpty() && filterOccurred)
            return new SelectResponse();

        HashSet<Integer> indexes = null;
        try {
            indexes = evaluatedIndexes.pop();
        } catch (NoSuchElementException e) {
            for (Column column : toShow) {
                indexes = column.getAllIndexes();
                break;
            }
        }

        // Building response
        for (Integer index : indexes) {
            boolean pass = false;
            for (HashSet<Integer> indexSet : evaluatedIndexes) {
                if (!indexSet.contains(index)) {
                    pass = true;
                    break;
                }
            }
            if (!pass)
                continue;
            HashMap<String, Object> row = new HashMap<>();
            for (Column column : toShow) {
                LambdaTypeConverter converter = column.getConverter();
                row.put(column.getName(), converter.call(String.valueOf(column.getValue(index))));
            }
            selectResponse.add(index, row);
        }
        return selectResponse;
    }

}
