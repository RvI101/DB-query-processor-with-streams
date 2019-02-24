package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

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
            String schemaName = null;
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
        return null;
    }
}
