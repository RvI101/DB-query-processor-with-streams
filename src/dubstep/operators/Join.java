package dubstep.operators;

import net.sf.jsqlparser.expression.Expression;

public class Join extends Operator {
    private JoinType type;
    private Expression condition;

    public Join(net.sf.jsqlparser.statement.select.Join sqlJoin) {
        this.type = pickType(sqlJoin);
        this.condition = sqlJoin.getOnExpression();
    }

    public Join(JoinType type, Expression condition) {
        this.type = type;
        this.condition = condition;
    }

    public JoinType getType() {
        return type;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    private JoinType pickType(net.sf.jsqlparser.statement.select.Join join) {
        if(join.isSimple())
            return JoinType.SIMPLE;
        else if(join.isNatural())
            return JoinType.NATURAL;
        else
            return JoinType.FULL;
    }
}
