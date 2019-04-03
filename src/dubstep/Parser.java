package dubstep;

import dubstep.operators.*;
import dubstep.operators.Limit;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Union;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {
    public static int mode;
    static void parseAndEvaluate(String modeString) throws ParseException {
        mode = modeString.contains("mem") ? 1 : 2;
        CCJSqlParser parser = new CCJSqlParser(System.in);
        System.out.print("$>");
        Statement statement;
        while ((statement = parser.Statement()) != null) {
            System.out.println("");
            if(statement instanceof Select) {
                Objects.requireNonNull(evaluateTree(parseStatement(((Select) statement).getSelectBody(), null)))
                        .forEach(
                        t -> {
                            String tupleString = t.stream().map(Cell::toString).collect(Collectors.joining("|"));
                            System.out.println(tupleString);
                        }
                );
            }
            else if(statement instanceof CreateTable) {
                TableScan.createTable((CreateTable)statement);
            }
            System.out.print("$>");
        }
    }

    private static Operator parseStatement(SelectBody selectBody, String alias) {
        if(selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            Operator root = null;
            Operator node = null;
            //Alias operator
            if(alias != null) {
                root = new Alias(alias);
                node = root;
            }
            // Limit operator
            if(plainSelect.getLimit() != null) {
                if(root == null) {
                    root = new Limit(plainSelect.getLimit().getRowCount());
                    node = root;
                }
                else
                    node = node.cascade(new Limit(plainSelect.getLimit().getRowCount()));
            }
            // Sort operator
            if(plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty()) {
                if(root == null) {
                    root = new Sort(plainSelect.getOrderByElements());
                    node = root;
                }
                else
                    node = node.cascade(new Sort(plainSelect.getOrderByElements()));
            }
            // Project or Aggregate operator
            if(root == null) {
                if(hasAgg(plainSelect.getSelectItems())) {
                    root = new Aggregation(plainSelect.getSelectItems()
                            .stream()
                            .map(i -> ((SelectExpressionItem)i).getExpression())
                            .collect(Collectors.toList()), plainSelect.getGroupByColumnReferences(), plainSelect.getHaving());
                }
                else
                    root = new Projection(plainSelect.getSelectItems());
                node = root;
            }
            else {
                if(hasAgg(plainSelect.getSelectItems())) {
                    node = node.cascade(new Aggregation(plainSelect.getSelectItems()
                            .stream()
                            .map(i -> ((SelectExpressionItem)i).getExpression())
                            .collect(Collectors.toList()), plainSelect.getGroupByColumnReferences(), plainSelect.getHaving()));
                }
                else
                    node = node.cascade(new Projection(plainSelect.getSelectItems()));
            }
            // Select operator
            if (plainSelect.getWhere() != null) {
                Selection selection = new Selection(plainSelect.getWhere());
                node = node.cascade(selection);
            }
            if(plainSelect.getFromItem() instanceof Table) {
                // Table Scan
                Table table = (Table) plainSelect.getFromItem();
                TableScan tableScan = new TableScan(table);
                if(plainSelect.getJoins() == null || plainSelect.getJoins().isEmpty()) {
                    //Simple select statement with only one table
                    node = node.cascade(tableScan);
                    return root;
                }
                if(plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
                   //Cross Product
                    node = node.cascade(parseJoins(plainSelect.getJoins(), tableScan));
                    return root;
                }

            }
            else if(plainSelect.getFromItem() instanceof SubSelect){
                //SubSelect
                SubSelect subSelect = (SubSelect) plainSelect.getFromItem();
                if(plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
                    node = node.cascade(parseJoins(plainSelect.getJoins(), parseStatement(subSelect.getSelectBody(), subSelect.getAlias())));
                }
                else
                    node = node.cascade(parseStatement(subSelect.getSelectBody(), subSelect.getAlias()));
            }
            return root;
        }
        else {
            Union union = (Union) selectBody;
            dubstep.operators.Union root = new dubstep.operators.Union();
            Operator node = root;
            int num = union.getPlainSelects().size();
            int i = 0;
            //Union
            for(PlainSelect plainSelect : union.getPlainSelects()) {
                ((dubstep.operators.Union) node).setLeft(parseStatement(plainSelect, null));
                ((dubstep.operators.Union) node).setRight(new dubstep.operators.Union());
                node = ((dubstep.operators.Union) node).getRight();
                if(num - (++i) <= 2)
                    break;
            }
            ((dubstep.operators.Union) node).setLeft(parseStatement(union.getPlainSelects().get(i), null));
            ((dubstep.operators.Union) node).setRight(parseStatement(union.getPlainSelects().get(i + 1), null));
            return root;
        }
    }

    private static Operator parseJoins(List<Join> joins, Operator o) {
        //Cross Product
        CrossProduct root = new CrossProduct();
        Operator node = root;
        int num = joins.size();
        int i = 0;
        if(num == 1) {
            ((CrossProduct)node).setLeft(o);
            Join join = joins.get(0);
            if(join.getRightItem() instanceof Table)
                ((CrossProduct)node).setRight(new TableScan((Table) join.getRightItem()));
            else if(join.getRightItem() instanceof SubSelect)
                ((CrossProduct) node).setRight(parseStatement(((SubSelect) join.getRightItem()).getSelectBody(), join.getRightItem().getAlias()));
        }
        else {
            List<Join> joinList = new ArrayList<>(joins);
            Collections.reverse(joinList); // reversing join list to create left deep nested cross product
            for (Join join : joinList) {
                if(join.getRightItem() instanceof Table)
                    ((CrossProduct)node).setRight(new TableScan((Table) join.getRightItem()));
                else if(join.getRightItem() instanceof SubSelect)
                    ((CrossProduct) node).setRight(parseStatement(((SubSelect) join.getRightItem()).getSelectBody(), join.getRightItem().getAlias()));
                ((CrossProduct) node).setLeft(new CrossProduct());
                node = ((CrossProduct) node).getLeft();
                if (num - (++i) <= 1)
                    break;
            }
            ((CrossProduct) node).setLeft(o);
            Join join = joinList.get(num - 1);
            if(join.getRightItem() instanceof Table)
                ((CrossProduct)node).setRight(new TableScan((Table) join.getRightItem()));
            else if(join.getRightItem() instanceof SubSelect)
                ((CrossProduct) node).setRight(parseStatement(((SubSelect) join.getRightItem()).getSelectBody(), join.getRightItem().getAlias()));
        }
        return root;
    }

    private static Stream<List<Cell>> evaluateTree(Operator node) {
        if(node instanceof Selection)
            return ((Selection) node).evaluate(evaluateTree(((Selection) node).getChild()));
        else if(node instanceof Projection)
            return ((Projection) node).evaluate(evaluateTree(((Projection) node).getChild()));
        else if(node instanceof Sort)
            return ((Sort) node).evaluate(evaluateTree(((Sort) node).getChild()));
        else if(node instanceof Limit)
            return ((Limit) node).evaluate(evaluateTree(((Limit) node).getChild()));
        else if(node instanceof Aggregation)
            return ((Aggregation) node).evaluate(evaluateTree(((Aggregation) node).getChild()));
        else if(node instanceof Alias)
            return ((Alias) node).evaluate(evaluateTree(((Alias) node).getChild()));
        else if(node instanceof CrossProduct) {
            return ((CrossProduct) node)
                    .evaluate(evaluateTree(((CrossProduct) node).getLeft())
                            , evaluateTree(((CrossProduct) node).getRight()));
        }
        else if(node instanceof dubstep.operators.Union) {
            return ((dubstep.operators.Union) node)
                    .evaluate(evaluateTree(((dubstep.operators.Union) node).getLeft())
                            , evaluateTree(((dubstep.operators.Union) node).getRight()));

        }
        else if(node instanceof TableScan) {
            return ((TableScan) node).tableScan();
        }
        return null;
    }

//    private Operator optimize(Operator node) {
//        if(node instanceof Selection) {
//            List<Expression> expressions = splitAndClauses(((Selection) node).getCondition());
//            if(((Selection) node).getChild() instanceof CrossProduct) {
//                CrossProduct crossProduct = (CrossProduct) ((Selection) node).getChild();
//                Operator
//            }
//        }
//    }

    private static boolean hasAgg(List<SelectItem> selectItems) {
        return selectItems.stream().anyMatch(item ->
            {
                return item instanceof SelectExpressionItem && ((SelectExpressionItem) item).getExpression() instanceof Function;
            });
    }

    private List<Expression> splitAndClauses(Expression e)
    {
        List<Expression> ret = new ArrayList<>();
        if(e instanceof AndExpression){
            AndExpression a = (AndExpression)e;
            ret.addAll(
                    splitAndClauses(a.getLeftExpression())
            );
            ret.addAll(
                    splitAndClauses(a.getRightExpression())
            );
        } else {
            ret.add(e);
        }
        return ret;
    }

    public static String typeOf(PrimitiveValue value) {
        switch(value.getType()) {
            case LONG:
                return "int";
            case DOUBLE:
                return "decimal";
            case STRING:
                return "string";
            case BOOL:
                return "bool";
            case DATE:
                return "date";
            default:
                return "string";
        }
    }

    public static List<Cell> parseTuple(String tupleString, Map<String, String> tupleSchema) {
        List<String> typeList = new ArrayList<>(tupleSchema.values());
        List<String> colList = new ArrayList<>(tupleSchema.keySet());
        int i = 0;
        List<Cell> tuple = new ArrayList<>();
        for(String cell : tupleString.split("\\|")) {
            switch (typeList.get(i)) {
                case "string":
                case "varchar":
                case "char":
                    tuple.add(new Cell(colList.get(i), new StringValue(cell)));
                    break;
                case "int":
                    tuple.add(new Cell(colList.get(i), new LongValue(cell)));
                    break;
                case "decimal":
                    tuple.add(new Cell(colList.get(i), new DoubleValue(cell)));
                    break;
                case "date":
                    tuple.add(new Cell(colList.get(i), new DateValue(cell)));
                    break;
                default:
                    System.out.println("Could not parse cell " + cell);
                    tuple.add(new Cell(colList.get(i), new StringValue(cell)));
                    break;
            }
            i++;
        }
        return tuple;
    }

}
