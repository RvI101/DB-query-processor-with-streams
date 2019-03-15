package dubstep.operators;

import dubstep.Cell;
import dubstep.TupleEval;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;
import java.util.List;

public class Selection extends Operator{
    Expression condition;
    Operator child;

    public Selection(Expression condition, Operator child) {
        this.condition = condition;
        this.child = child;
    }

    public Selection(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public static boolean doSelect(List<Cell> tuple, Expression expression) {
        TupleEval tupleEval = getEval(tuple);

        try {
            return (tupleEval.eval(expression).toBool());
        } catch (SQLException e) {
            System.out.println("Error doing select");
            return false;
        }
    }
}
