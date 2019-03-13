package dubstep.operators;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;

public class Aggregation {
    private AggregationType type;
    private Expression onExpression;
    private List<Expression> groupBy;
    private Expression having;

    public Aggregation(AggregationType type, Expression onExpression, List<Expression> groupBy, Expression having) {
        this.type = type;
        this.onExpression = onExpression;
        this.groupBy = groupBy;
        this.having = having;
    }

    public AggregationType getType() {
        return type;
    }

    public void setType(AggregationType type) {
        this.type = type;
    }

    public Expression getOnExpression() {
        return onExpression;
    }

    public void setOnExpression(Expression onExpression) {
        this.onExpression = onExpression;
    }

    public List<Expression> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<Expression> groupBy) {
        this.groupBy = groupBy;
    }

    public Expression getHaving() {
        return having;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }
}