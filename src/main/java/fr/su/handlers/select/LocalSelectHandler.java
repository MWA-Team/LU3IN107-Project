package fr.su.handlers.select;

import fr.su.controllers.TableSelection;
import fr.su.database.Column;
import fr.su.database.Database;
import fr.su.handlers.select.response.SelectResponse;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class LocalSelectHandler implements SelectHandler {

    @Override
    public SelectResponse select(TableSelection.SelectBody selectBody) {

        List<String> toShow = selectBody.getColumns();
        List<String> columnsToEvaluate = selectBody.getWhere().keySet().stream().toList();
        SelectResponse selectResponse = new SelectResponse();

        Database database = Database.getInstance();
        for(Column column : database.getTables().get(selectBody.getTable()).getColumns().values()) {

            Column newColumn = new Column(column.getName(), String.class, true);

            if(columnsToEvaluate.contains(column.getName()) || toShow.contains(column.getName())) {

                selectResponse.getColumns().add(newColumn);

                if(columnsToEvaluate.contains(column.getName())) {

                    String compare = selectBody.getWhere().get(column.getName()).getValue();
                    TableSelection.Operand operand = selectBody.getWhere().get(column.getName()).getOperand();

                    if(operand.equals(TableSelection.Operand.EQUALS)) {

                        for(int i = 0; i < column.getValues().size(); i++) {

                            String val = (String) column.getValues().get(i);
                            if(val.equals(compare)) {
                                System.out.println("addeing " + val);
                                newColumn.addValue(i, val);
                                selectResponse.getIndexes().add(i);
                            }
                        }
                    }



                }
            }
        }

        return selectResponse;
    }
}
