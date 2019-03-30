package dubstep.operators;

import dubstep.Cell;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregation extends Operator{
    private List<Expression> onExpression;
    private String alias;
    private List<Expression> groupBy;
    private Expression having;
    Operator child;

    public Aggregation(List<Expression> onExpression, List<Expression> groupBy, Expression having) {
        this.onExpression = onExpression;
        this.groupBy = groupBy;
        this.having = having;
    }

    public Aggregation(List<Expression> onExpression, String alias, List<Expression> groupBy, Expression having, Operator child) {
        this.onExpression = onExpression;
        this.alias = alias;
        this.groupBy = groupBy;
        this.having = having;
        this.child = child;
    }

    public Aggregation(List<Expression> onExpression, List<Expression> groupBy, Expression having, Operator child) {
        this.onExpression = onExpression;
        this.groupBy = groupBy;
        this.having = having;
        this.child = child;
    }

    public List<Expression> getOnExpression() {
        return onExpression;
    }

    public void setOnExpression(List<Expression> onExpression) {
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

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tuples) {
        List<List<Cell>> tupleList = tuples.collect(Collectors.toList());
        List<Cell> aggTuple = new ArrayList<>();
        for (Expression expression : onExpression) {
            Function fn = (Function) expression;
            switch (((Function)fn).getName()) {
                case "AVG":
                    DoubleValue average = new DoubleValue(tupleList.stream().map(tuple -> {
                        try {
                            return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
                        } catch (SQLException e) {
                            System.out.println("Averaging Error");
                            return 0L;
                        }
                    }).collect(Collectors.averagingLong(l -> l)));
                    Cell avg = new Cell(alias != null ? alias : fn.toString(), average);
                    aggTuple.add(avg);
                    break;

                case "SUM":
                    LongValue sum = new LongValue(tupleList.stream().map(tuple -> {
                        try {
                            return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
                        } catch (SQLException e) {
                            System.out.println("Summing Error");
                            return 0L;
                        }
                    }).reduce((a, b) -> a + b).orElse(0L));
                    Cell sm = new Cell(alias != null ? alias : fn.toString(), sum);
                    aggTuple.add(sm);
                    break;

                case "COUNT":
                    LongValue count = new LongValue((long) tupleList.size()); //TODO: Clean
                    Cell cnt = new Cell(alias != null ? alias : fn.toString(), count);
                    aggTuple.add(cnt);
                    break;

                case "MAX":
                    LongValue max = new LongValue(tupleList.stream().map(tuple -> {
                        try {
                            return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
                        } catch (SQLException e) {
                            System.out.println("Maxing Error");
                            return 0L;
                        }
                    }).max(Comparator.naturalOrder()).orElse(0L));
                    Cell mx = new Cell(alias != null ? alias : fn.toString(), max);
                    aggTuple.add(mx);
                    break;

                case "MIN":
                    LongValue min = new LongValue(tupleList.stream().map(tuple -> {
                        try {
                            return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
                        } catch (SQLException e) {
                            System.out.println("Mining Error");
                            return 0L;
                        }
                    }).min(Comparator.naturalOrder()).orElse(0L));
                    Cell mn = new Cell(alias != null ? alias : fn.toString(), min);
                    aggTuple.add(mn);
                    break;
            }
        }
        return Stream.of(aggTuple);
    }
}
