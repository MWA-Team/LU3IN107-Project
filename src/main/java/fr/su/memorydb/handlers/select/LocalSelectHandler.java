package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) throws IOException {
        HashSet<Column> toShow = new HashSet<>();
        HashSet<Column> toEvaluate = new HashSet<>();
        LinkedList<int[]> evaluatedIndexes = new LinkedList<>();
        SelectResponse selectResponse = new SelectResponse(selectBody.getTable());

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
            return new SelectResponse(selectBody.getTable());

        int[] indexes = null;
        if (!evaluatedIndexes.isEmpty()) {
            int min = -1;
            int index = 0;
            for (int i = 0; i < evaluatedIndexes.size(); i++) {
                int len = evaluatedIndexes.get(i).length;
                if (min > len || min == -1) {
                    min = len;
                    index = i;
                }
            }
            indexes = evaluatedIndexes.get(index);
        }

        int start = indexes != null ? indexes[0] : 0;
        int end = indexes != null ? indexes[indexes.length - 1] : Database.getInstance().getTables().get(selectBody.getTable()).rowsCounter;
        HashMap<Column, Object[]> values = new HashMap<>();

        // Building response
        if (indexes == null) {
            for (int index = 0; index < Database.getInstance().getTables().get(selectBody.getTable()).rowsCounter; index++) {
                filterIndexes(toShow, evaluatedIndexes, selectResponse, start, end, values, index);
            }
        } else {
            for (Integer index : indexes) {
                filterIndexes(toShow, evaluatedIndexes, selectResponse, start, end, values, index);
            }
        }
          
        //Managing group by : decrementing in negative numbers
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


        return selectResponse;
    }

    private void filterIndexes(HashSet<Column> toShow, LinkedList<int[]> evaluatedIndexes, SelectResponse selectResponse, int start, int end, HashMap<Column, Object[]> values, int index) throws IOException {
        boolean pass = false;

        for (int[] indexSet : evaluatedIndexes) {
            int found = Arrays.binarySearch(indexSet, index);
            if (found < 0) {
                pass = true;
                break;
            }
        }

        if (!evaluatedIndexes.isEmpty() && pass)
            return;

        HashMap<String, Object> row = new HashMap<>();
        for (Column column : toShow) {
            if (values.get(column) == null)
                values.put(column, column.getValues(start, end));
            row.put(column.getName(), values.get(column)[index - start]);
        }
        selectResponse.add(index, row);
    }

}
