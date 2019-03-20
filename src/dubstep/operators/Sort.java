package dubstep.operators;

import dubstep.Cell;
import dubstep.TupleEval;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sort extends Operator {
    Operator child;
    List<OrderByElement> columns;

    public Sort() {
    }

    public Sort(List<OrderByElement> columns) {
        this.columns = columns;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public List<OrderByElement> getColumns() {
        return columns;
    }

    public void setColumns(List<OrderByElement> columns) {
        this.columns = columns;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tupleStream) {
        List<List<Cell>> tuples = tupleStream.collect(Collectors.toList());
        OrderByElement orderByElement = columns.get(0);

        tuples.sort(Comparator.comparing(tuple -> {
            try {
                return getEval(tuple).eval(orderByElement.getExpression()).toLong();
            } catch (SQLException e) {
                System.out.println("Sorting Error");
                return 0L;
            }
        }));

        return tuples.stream();
    }
}
