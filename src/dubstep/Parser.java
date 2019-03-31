package dubstep;

import dubstep.operators.*;
import dubstep.operators.Limit;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Union;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {
    static void parseAndEvaluate() throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(System.in);
        System.out.print("$>");
        Statement statement;
        while ((statement = parser.Statement()) != null) {
            if(statement instanceof Select) {
                Objects.requireNonNull(evaluateTree(parseStatement(((Select) statement).getSelectBody())))
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
//    private static Stream<List<Cell>> handleSelect(SelectBody selectBody) {
//        if(selectBody instanceof PlainSelect) {
//            PlainSelect plainSelect = (PlainSelect)selectBody;
//            Stream<List<Cell>> tuples = null;
//            if(plainSelect.getFromItem() instanceof Table) {
//                Table table = (Table) plainSelect.getFromItem();
//                String tableAlias = table.getAlias() != null ? table.getAlias() : table.getName();
//                tuples = Evaluator.tableScan(table.getName(), tableAlias);
//                if(plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
//                    for(Join join : plainSelect.getJoins()) {
//                        String joinAlias = join.getRightItem().getAlias() != null
//                                ? join.getRightItem().getAlias() : ((Table)join.getRightItem()).getName();
//                        tuples = Objects.requireNonNull(tuples)
//                                .flatMap(tuple -> Evaluator.tableScan(((Table)join.getRightItem()).getName(), joinAlias)
//                                        .map(t -> {t.addAll(tuple); return t;}));
//                    }
//                }
//            }
//            else if(plainSelect.getFromItem() instanceof SubSelect){
//                tuples = handleSelect(((SubSelect)plainSelect.getFromItem()).getSelectBody());
//            }
//            return Objects.requireNonNull(tuples).filter(t -> Evaluator.doSelect(t, plainSelect.getWhere()))
//                    .map(t -> Evaluator.doProject(t, plainSelect.getSelectItems()));
//        }
//        else {
//            Union union = (Union) selectBody;
//            Stream<List<Cell>> results = Stream.empty();
//            for(PlainSelect plainSelect : union.getPlainSelects()) {
//                results = Stream.concat(results, handleSelect(plainSelect));
//            }
//            return results;
//        }
//    }

    private static Operator parseStatement(SelectBody selectBody) {
        if(selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            Operator root = null;
            Operator node = null;
            // Limit operator
            if(plainSelect.getLimit() != null) {
                root = new Limit(plainSelect.getLimit().getRowCount());
                node = root;
            }
            // Sort operator
            if(plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty()) {
                if(root == null) {
                    root = new Sort(plainSelect.getOrderByElements());
                    node = root;
                }
                else {
                    node = node.cascade(new Sort(plainSelect.getOrderByElements()));
                }
            }
            // Project or Aggregate operator
            if(root == null) {
                if((plainSelect.getGroupByColumnReferences() == null || plainSelect.getGroupByColumnReferences().isEmpty())
                        && hasAgg(plainSelect.getSelectItems())) {
                    root = new Aggregation(plainSelect.getSelectItems()
                            .stream()
                            .map(i -> ((SelectExpressionItem)i).getExpression())
                            .collect(Collectors.toList()), null, null);
                }
                else
                    root = new Projection(plainSelect.getSelectItems());
                node = root;
            }
            else {
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
                    CrossProduct crossProduct = new CrossProduct();
                    node = node.cascade(crossProduct);
                    int num = plainSelect.getJoins().size();
                    int i = 0;
                    if(num == 1) {
                        ((CrossProduct)node).setLeft(tableScan);
                        ((CrossProduct)node).setRight(new TableScan((Table) plainSelect.getJoins().get(0).getRightItem()));
                    }
                    else {
                        List<Join> joinList = new ArrayList<>(plainSelect.getJoins());
                        Collections.reverse(joinList); // reversing join list to create left deep nested cross product
                        for (Join join : joinList) {
                            ((CrossProduct) node).setRight(new TableScan((Table) join.getRightItem()));
                            ((CrossProduct) node).setLeft(new CrossProduct());
                            node = ((CrossProduct) node).getLeft();
                            if (num - (++i) <= 1)
                                break;
                        }
                        ((CrossProduct) node).setLeft(tableScan);
                        ((CrossProduct) node).setRight(new TableScan((Table) joinList.get(num - 1).getRightItem()));
                    }
                    return root;
                }

            }
            else if(plainSelect.getFromItem() instanceof SubSelect){
                //SubSelect
                node = node.cascade(parseStatement(((SubSelect) plainSelect.getFromItem()).getSelectBody()));
            }
            return root;
        }
        else {
            Union union = (Union) selectBody;
            dubstep.operators.Union root = new dubstep.operators.Union();
            Operator node = root;
            int num = union.getPlainSelects().size();
            int i = 1;
            //Union
            for(PlainSelect plainSelect : union.getPlainSelects()) {
                ((dubstep.operators.Union) node).setLeft(parseStatement(plainSelect));
                ((dubstep.operators.Union) node).setRight(new dubstep.operators.Union());
                node = ((dubstep.operators.Union) node).getRight();
                if(++i > num - 2)
                    break;
            }
            ((dubstep.operators.Union) node).setLeft(parseStatement(union.getPlainSelects().get(i - 1)));
            ((dubstep.operators.Union) node).setRight(parseStatement(union.getPlainSelects().get(i)));

            return root;
        }
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
        else if(node instanceof Aggregation) {
            return ((Aggregation) node).evaluate(evaluateTree(((Aggregation) node).getChild()));
        }
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

    private static boolean hasAgg(List<SelectItem> selectItems) {
        return selectItems.stream().anyMatch(item ->
            {
                return item instanceof SelectExpressionItem && ((SelectExpressionItem) item).getExpression() instanceof Function;
            });
    }

}
