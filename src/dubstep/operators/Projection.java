package dubstep.operators;

import dubstep.Cell;
import dubstep.TupleEval;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Projection extends Operator {
    List<SelectItem> items;
    Operator child;

    public Projection(List<SelectItem> items, Operator child) {
        this.items = items;
        this.child = child;
    }

    public Projection(List<SelectItem> items) {
        this.items = items;
    }

    public List<SelectItem> getItems() {
        return items;
    }

    public void setItems(List<SelectItem> items) {
        this.items = items;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tuples) {
        return tuples.map(t -> doProject(t, getItems()));
    }

    public static List<Cell> doProject(List<Cell> tuple, List<SelectItem> selectItems){
        TupleEval tupleEval = getEval(tuple);
        if(selectItems.get(0) instanceof AllColumns) {
            return tuple;   //Global Wildcard, can return as we know it is the only SelectItem
        }
        List<Cell> projectedTuple = new ArrayList<>();
        for(SelectItem selectItem : selectItems) {
            if(selectItem instanceof AllTableColumns) {
                AllTableColumns allTableColumns = (AllTableColumns) selectItem;
                projectedTuple.addAll(tuple.stream().filter(c -> c.getTable().equals(allTableColumns.getTable().getName())).collect(Collectors.toList()));
            }
            if(selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                try {
                    String colName = ((Column) selectExpressionItem.getExpression()).getWholeColumnName();
                    projectedTuple.add(new Cell(colName, tupleEval.eval(selectExpressionItem.getExpression()), selectExpressionItem.getAlias()));
                } catch (SQLException e) {
                    System.out.println("Error doing project");
                }
            }
        }
        return projectedTuple;
    }
}
