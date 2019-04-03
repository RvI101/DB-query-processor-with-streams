package dubstep.operators;

import dubstep.Cell;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregation extends Operator {
    private List<Expression> onExpression;
    private String alias; //TODO Clean
    private List<Column> groupBy;
    private Expression having;
    Operator child;

    public Aggregation(List<Expression> onExpression, List<Column> groupBy, Expression having) {
        this.onExpression = onExpression;
        this.groupBy = groupBy;
        this.having = having;
    }

    public Aggregation(List<Expression> onExpression, String alias, List<Column> groupBy, Expression having, Operator child) {
        this.onExpression = onExpression;
        this.alias = alias;
        this.groupBy = groupBy;
        this.having = having;
        this.child = child;
    }

    public Aggregation(List<Expression> onExpression, List<Column> groupBy, Expression having, Operator child) {
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

    public List<Column> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<Column> groupBy) {
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
        if(groupBy != null && !groupBy.isEmpty()) {
            return groupedEvaluate(tupleList);
        }
        return Stream.of(evaluateGroup(tupleList));
    }
    public List<Cell> evaluateGroup(List<List<Cell>> tupleList) {
        List<Cell> aggTuple = new ArrayList<>();
        List<Cell> refTuple = tupleList.get(0);
        if(groupBy != null) {
            for (Expression expression : groupBy) {
                try {
                    aggTuple.add(new Cell(expression.toString(), getEval(refTuple).eval(expression)));
                } catch (SQLException e) {
                    System.out.println("Group error");
                }
            }
        }
        for (Expression expression : onExpression) {
            if(!(expression instanceof Function))
                continue;
            Function fn = (Function) expression;
            switch (fn.getName()) {
                case "AVG":
                    aggTuple.add(calculateAverage(tupleList, fn));
                    break;

                case "SUM":
                    aggTuple.add(calculateSum(tupleList, fn));
                    break;

                case "COUNT":
                    LongValue count = new LongValue((long) tupleList.size()); //TODO: Clean
                    Cell cnt = new Cell(alias != null ? alias : fn.toString(), count);
                    aggTuple.add(cnt);
                    break;

                case "MAX":
                    aggTuple.add(calculateMax(tupleList, fn));
                    break;

                case "MIN":
                    aggTuple.add(calculateMin(tupleList, fn));
                    break;
            }
        }
        return aggTuple;
    }
    private Stream<List<Cell>> groupedEvaluate(List<List<Cell>> tupleList) {
        tupleList.sort(tupleGroupComparator);
        List<List<Cell>> groupedTuples = new ArrayList<>();
        Iterator<List<Cell>> it = tupleList.iterator();
        List<Cell> refTuple = it.next();
        List<List<Cell>> group = new ArrayList<>();
        group.add(refTuple);
        while(it.hasNext()) {
           List<Cell> tuple = it.next();
           if(tupleGroupComparator.compare(refTuple, tuple) != 0) {
               List<Cell> gTuple = evaluateGroup(group);
               try {
                   if(having != null && getEval(gTuple).eval(having).toBool()) //TODO: Evaluate Functions in having not already present as a Column
                       groupedTuples.add(evaluateGroup(group));
                   else if(having == null) {
                       groupedTuples.add(evaluateGroup(group));
                   }
               } catch (SQLException e) {
                   System.out.println("Having error " + e.getMessage());
               }
               group = new ArrayList<>();
           }
           refTuple = tuple;
           group.add(tuple);
        }
        return groupedTuples.stream();
    }

    private Comparator<List<Cell>> tupleGroupComparator = new Comparator<List<Cell>>() {
        @Override
        public int compare(List<Cell> a, List<Cell> b) {
            int val = 0;
            for(Expression expression : groupBy) {
                try {
                    PrimitiveValue first = getEval(a).eval(expression);
                    PrimitiveValue second = getEval(b).eval(expression);
                    switch(first.getType()) {
                        case LONG:
                            val = Long.compare(first.toLong(), second.toLong());
                            if(val == 0)
                                continue;
                            return val;
                        case DOUBLE:
                            val = Double.compare(first.toDouble(), second.toDouble());
                            if(val == 0)
                                continue;
                            return val;
                        case STRING:
                            val = first.toString().compareTo(second.toString());
                            if(val == 0)
                                continue;
                            return val;
                        case BOOL:
                            val = Boolean.compare(first.toBool(),second.toBool());
                            if(val == 0)
                                continue;
                            return val;
                        case DATE:
                            val = ((DateValue)first).getValue().compareTo(((DateValue)second).getValue());
                            if(val == 0)
                                continue;
                            return val;
                        case TIMESTAMP:
                            val = ((TimestampValue)first).getValue().compareTo(((TimestampValue)second).getValue());
                            if(val == 0)
                                continue;
                            return val;
                        case TIME:
                            val = ((TimeValue)first).getValue().compareTo(((TimeValue)second).getValue());
                            if(val == 0)
                                continue;
                            return val;
                    }
                } catch (SQLException e) {
                    System.out.println("Sorting error in grouping");
                }
            }
            return val;
        }
    };

    private Cell calculateAverage(List<List<Cell>> tupleList, Function fn) {
        DoubleValue average = new DoubleValue(tupleList.stream().map(tuple -> {
            try {
                return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
            } catch (SQLException e) {
                System.out.println("Averaging Error");
                return 0L;
            }
        }).collect(Collectors.averagingLong(l -> l)));
        return new Cell(alias != null ? alias : fn.toString(), average);
    }

    private Cell calculateSum(List<List<Cell>> tupleList, Function fn) {
        LongValue sum = new LongValue(tupleList.stream().map(tuple -> {
            try {
                return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
            } catch (SQLException e) {
                System.out.println("Summing Error");
                return 0L;
            }
        }).reduce((a, b) -> a + b).orElse(0L));
        return new Cell(alias != null ? alias : fn.toString(), sum);
    }

    private Cell calculateMax(List<List<Cell>> tupleList, Function fn) {
        LongValue max = new LongValue(tupleList.stream().map(tuple -> {
            try {
                return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
            } catch (SQLException e) {
                System.out.println("Maxing Error");
                return 0L;
            }
        }).max(Comparator.naturalOrder()).orElse(0L));
        return new Cell(alias != null ? alias : fn.toString(), max);
    }

    private Cell calculateMin(List<List<Cell>> tupleList, Function fn) {
        LongValue min = new LongValue(tupleList.stream().map(tuple -> {
            try {
                return getEval(tuple).eval(fn.getParameters().getExpressions().get(0)).toLong();
            } catch (SQLException e) {
                System.out.println("Mining Error");
                return 0L;
            }
        }).min(Comparator.naturalOrder()).orElse(0L));
        return new Cell(alias != null ? alias : fn.toString(), min);
    }
}
