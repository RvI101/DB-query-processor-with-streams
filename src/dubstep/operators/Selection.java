package dubstep.operators;

import net.sf.jsqlparser.expression.Expression;

public class Selection extends Operator{
    Expression condition;

    public Selection(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }
}
