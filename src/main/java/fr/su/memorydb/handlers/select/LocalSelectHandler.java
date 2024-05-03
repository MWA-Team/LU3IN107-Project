package fr.su.memorydb.handlers.select;

import fr.su.memorydb.controllers.TableSelection;
import fr.su.memorydb.database.Column;
import fr.su.memorydb.database.Database;
import fr.su.memorydb.handlers.select.response.SelectResponse;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) {
        List<String> toShow = selectBody.getColumns();
        List<String> columnsToEvaluate = selectBody.getWhere().keySet().stream().toList();
        SelectResponse selectResponse = new SelectResponse();

        Database database = Database.getInstance();
        List<Column> columns = new ArrayList<>();

        //We load firstly columns that will reduce amount of quantity
        for(Column columns1 : database.getTables().get(selectBody.getTable()).getColumns()) {
            if(selectBody.getWhere().keySet().contains(columns1.getName())) {
                columns.add(columns1);
            }
        }

        //We load columns without any 'where' clause after
        for(Column columns1 : database.getTables().get(selectBody.getTable()).getColumns()) {
            if(selectBody.getColumns().contains(columns1.getName())) {
                if (!columns.contains(columns1))
                    columns.add(columns1);
            }
        }

        for(Column column : columns) {
            Column newColumn = new Column(column.getName(), true, column.getType());

            selectResponse.getColumns().add(newColumn);

            if(columnsToEvaluate.contains(column.getName())) {

                String compare = selectBody.getWhere().get(column.getName()).getValue();
                TableSelection.Operand operand = selectBody.getWhere().get(column.getName()).getOperand();

                if((columnsToEvaluate.contains(column.getName()) && operand.equals(TableSelection.Operand.EQUALS))) {
                    column.getRows().forEach((value, indexes) -> {
                        if (value.equals(compare)) {
                            ((HashSet<Integer>) indexes).forEach(index -> {
                                newColumn.addRowValue(value, index);
                                selectResponse.getIndexes().add(index);
                            });
                        }
                    });
                }

            } else if(toShow.contains(column.getName())){
                if(selectResponse.getIndexes().isEmpty()) {
                    column.getRows().forEach((value, indexes) -> {
                        ((HashSet<Integer>) indexes).forEach(index -> {
                            newColumn.addRowValue(value, index);
                            selectResponse.getIndexes().add(index);
                        });
                    });
                    continue;
                }

                column.getRows().forEach((value, indexes) -> {
                    ((HashSet<Integer>) indexes).forEach(index -> {
                        newColumn.addRowValue(value, index);
                    });
                });
            }
        }

        return selectResponse;
    }
}
