package dubstep.operators;

import dubstep.Cell;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin extends Operator {
    private Operator left;
    private Operator right;
    private EqualsTo condition;
    private Map<PrimitiveValue, List<Cell>> hashTable;

    private HashJoin() {
        hashTable = new HashMap<>();
    }
    public HashJoin(EqualsTo condition) {
        this();
        this.condition = condition;
    }

    public HashJoin(Operator left, Operator right, EqualsTo condition) {
        this();
        this.left = left;
        this.right = right;
        this.condition = condition;
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(EqualsTo condition) {
        this.condition = condition;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> first, Stream<List<Cell>> second) {

    }
}
