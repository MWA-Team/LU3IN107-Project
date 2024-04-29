package fr.su.handlers.select;

import fr.su.controllers.TableSelection;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.handlers.select.response.SelectResponse;
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
        for(Column columns1 : database.getTables().get(selectBody.getTable()).getColumns().values()) {
            if(selectBody.getWhere().keySet().contains(columns1.getName())) {
                columns.add(columns1);
            }
        }

        //We load columns without any 'where' clause after
        for(Column columns1 : database.getTables().get(selectBody.getTable()).getColumns().values()) {
            if(selectBody.getColumns().contains(columns1.getName())) {
                if (!columns.contains(columns1))
                    columns.add(columns1);
            }
        }

        for(Column column : columns) {
            Column newColumn = new Column(column.getName(), true);

            selectResponse.getColumns().add(newColumn);

            if(columnsToEvaluate.contains(column.getName())) {

                String compare = selectBody.getWhere().get(column.getName()).getValue();
                TableSelection.Operand operand = selectBody.getWhere().get(column.getName()).getOperand();

                if((columnsToEvaluate.contains(column.getName()) && operand.equals(TableSelection.Operand.EQUALS))) {

                    for(Object o : column.getValues()) {
                        if(o.equals(compare)) {
                            for (Long index : (HashSet<Long>) column.getRows().get(o)) {
                                newColumn.addRow(o, index);
                                selectResponse.getIndexes().add(index);
                            }
                        }
                    }
                }

            } else if(toShow.contains(column.getName())){
                if(selectResponse.getIndexes().isEmpty()) {
                    for(Object o : column.getValues()) {
                        for (Long index : (HashSet<Long>) column.getRows().get(o)) {
                            newColumn.addRow(o, index);
                            selectResponse.getIndexes().add(index);
                        }
                    }
                    continue;
                }

                for(Object o : column.getValues()) {
                    for (Long index : (HashSet<Long>) column.getRows().get(o)) {
                        newColumn.addRow(o, index);
                    }
                }
            }
        }

        return selectResponse;
    }
}
