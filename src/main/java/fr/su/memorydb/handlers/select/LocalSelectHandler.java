package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.select.response.EmptySelectResponse;
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
            return new EmptySelectResponse();

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
        if (evaluatedIndexes.isEmpty() && !toEvaluate.isEmpty())
            return new SelectResponse();

        HashSet<Integer> indexes = null;
        if (!evaluatedIndexes.isEmpty())
            indexes = evaluatedIndexes.pop();
        else {
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
            if (!evaluatedIndexes.isEmpty() && pass)
                continue;
            HashMap<String, Object> row = new HashMap<>();
            for (Column column : toShow) {
                LambdaTypeConverter converter = column.getConverter();
                row.put(column.getName(), converter.call(String.valueOf(column.getValue(index))));
            }
            selectResponse.add(index, row);
        }

        //Managing group by : decrementing in negative numbers
        if(selectBody.hasGroupBy()) {

            int index = -1;
            String column = selectBody.getGroupBy();

            Column clm = Database.getInstance().getTables().get(selectBody.getTable()).getColumn(column);
            if(clm.stored()) {

                for(Object obj : clm.getRows().keySet()) { //Represent all distinct data in column

                    int groupByIndex = ((HashSet<Integer>)clm.getRows().get(obj)).iterator().next();
                    if(!selectResponse.containIndex(groupByIndex)) continue; //in case the where clause removed some of the group by values
                    HashMap base = new HashMap();

                    for(Column column1 : toShow) {
                        base.put(column1.getName(), groupByIndex); //we can't access specific index here, so we will access to it later in SelectResponse
                    }

                    selectResponse.add(index, base);
                    index-=1;

                }
            }
        }

        return selectResponse;
    }

}
