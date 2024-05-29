package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.utils.response.SelectResponse;
import fr.su.memorydb.utils.lambda.LambdaTypeConverter;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @ConfigProperty(name = "fr.su.blocs.size")
    int blocsSize;

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) throws IOException, InterruptedException {
        int blocsSize = this.blocsSize > 0 ? this.blocsSize : 1048576;
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
            int[] tmp = evaluatedIndexes.get(index);
            indexes = tmp != null ? tmp : new int[0];
        }

        // Building response
        HashMap<Column, Object[]> values = new HashMap<>();
        if (indexes == null) {
            int bloc = 0;
            for (int index = 0; index < Database.getInstance().getTables().get(selectBody.getTable()).rowsCounter; index++) {
                bloc = selectIndex(blocsSize, toShow, evaluatedIndexes, selectResponse, values, bloc, index);
            }
        } else {
            int bloc = 0;
            for (Integer index : indexes) {
                bloc = selectIndex(blocsSize, toShow, evaluatedIndexes, selectResponse, values, bloc, index);
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

    private int selectIndex(int blocsSize, HashSet<Column> toShow, LinkedList<int[]> evaluatedIndexes, SelectResponse selectResponse, HashMap<Column, Object[]> values, int bloc, int index) throws IOException {
        boolean pass = false;
        int retval = bloc;

        for (int[] indexSet : evaluatedIndexes) {
            int found = Arrays.binarySearch(indexSet, index);
            if (found < 0) {
                pass = true;
                break;
            }
        }

        if (!evaluatedIndexes.isEmpty() && pass)
            return retval;

        HashMap<String, Object> row = new HashMap<>();
        for (Column column : toShow) {
            if (index / blocsSize != bloc || values.get(column) == null) {
                retval = index / blocsSize;
                values.put(column, column.getValues(retval));
            }
            row.put(column.getName(), values.get(column)[index - retval * blocsSize]);
        }

        selectResponse.add(index, row);
        return retval;
    }

}
