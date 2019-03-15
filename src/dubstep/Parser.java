package dubstep;

import dubstep.operators.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.select.Join;

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
                handleSelect(((Select) statement).getSelectBody())
                        .forEach(
                        t -> {
                            String tupleString = t.stream().map(Cell::toString).collect(Collectors.joining("|"));
                            System.out.println(tupleString);
                        }
                );
            }
            else if(statement instanceof CreateTable) {
                Evaluator.createTable((CreateTable)statement);
            }
            System.out.print("$>");
        }

    }
    private static Stream<List<Cell>> handleSelect(SelectBody selectBody) {
        if(selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect)selectBody;
            Stream<List<Cell>> tuples = null;
            if(plainSelect.getFromItem() instanceof Table) {
                Table table = (Table) plainSelect.getFromItem();
                String tableAlias = table.getAlias() != null ? table.getAlias() : table.getName();
                tuples = Evaluator.tableScan(table.getName(), tableAlias);
                if(plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
                    for(Join join : plainSelect.getJoins()) {
                        String joinAlias = join.getRightItem().getAlias() != null
                                ? join.getRightItem().getAlias() : ((Table)join.getRightItem()).getName();
                        tuples = Objects.requireNonNull(tuples)
                                .flatMap(tuple -> Evaluator.tableScan(((Table)join.getRightItem()).getName(), joinAlias)
                                        .map(t -> {t.addAll(tuple); return t;}));
                    }
                }
            }
            else if(plainSelect.getFromItem() instanceof SubSelect){
                tuples = handleSelect(((SubSelect)plainSelect.getFromItem()).getSelectBody());
            }
            return Objects.requireNonNull(tuples).filter(t -> Evaluator.doSelect(t, plainSelect.getWhere()))
                    .map(t -> Evaluator.doProject(t, plainSelect.getSelectItems()));
        }
        else {
            Union union = (Union) selectBody;
            Stream<List<Cell>> results = Stream.empty();
            for(PlainSelect plainSelect : union.getPlainSelects()) {
                results = Stream.concat(results, handleSelect(plainSelect));
            }
            return results;
        }
    }

    private static Operator parseStatement(SelectBody selectBody) {
        if(selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            Projection projection = new Projection(plainSelect.getSelectItems());
            Operator node = projection;
            if (plainSelect.getWhere() != null) {
                Selection selection = new Selection(plainSelect.getWhere());
                node = node.cascade(selection);
            }
            if(plainSelect.getFromItem() instanceof Table) {
                // Table Scan
                Table table = (Table) plainSelect.getFromItem();
                String tableAlias = table.getAlias() != null ? table.getAlias() : table.getName();
                TableScan tableScan = new TableScan(tableAlias);
                if(plainSelect.getJoins() == null || plainSelect.getJoins().isEmpty()) {
                    //Simple select statement with only one table
                    node = node.cascade(tableScan);
                    return projection;
                }
                if(plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
                   //Cross Product
                    CrossProduct crossProduct = new CrossProduct();
                    Join join = plainSelect.getJoins().get(0);
                    String joinAlias = join.getRightItem().getAlias() != null
                            ? join.getRightItem().getAlias() : ((Table) join.getRightItem()).getName();
                    crossProduct.setLeft(tableScan);
                    crossProduct.setRight(new TableScan(joinAlias));
                    node.cascade(crossProduct);
                    return projection;
                }

            }
            else if(plainSelect.getFromItem() instanceof SubSelect){
                //SubSelect
            }
//            return Objects.requireNonNull(tuples).filter(t -> Evaluator.doSelect(t, plainSelect.getWhere()))
//                    .map(t -> Evaluator.doProject(t, plainSelect.getSelectItems()));
        }
        else {
            Union union = (Union) selectBody;
            //Union
            for(PlainSelect plainSelect : union.getPlainSelects()) {

            }

        }
        return null;
    }

}
