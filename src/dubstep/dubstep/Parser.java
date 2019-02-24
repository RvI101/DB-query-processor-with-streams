package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {
    public static void parseAndEvaluate(InputStream input) throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(input);
        System.out.print("$>");
        Statement statement;
        while ((statement = parser.Statement()) != null) {
            if(statement instanceof Select) {
                //Select handling
                Select select = (Select)statement;
                if(select.getSelectBody() instanceof PlainSelect) {
                    PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
                    if(plainSelect.getFromItem() instanceof Table) {    //Simple Table Scan
                        Table table = (Table)plainSelect.getFromItem();
                        Stream<List<PrimitiveValue>> tuples = Evaluator.tableScan(table.getName());
                        Objects.requireNonNull(tuples).filter(t -> Evaluator.doSelect(t, table.getName(), plainSelect.getWhere()))
                                .map(t -> Evaluator.doProject(t, table.getName(), plainSelect.getSelectItems()))
                                .forEach(
                                        t -> {
                                            String tupleString = t.stream().map(PrimitiveValue::toRawString).collect(Collectors.joining("|"));
                                            System.out.println(tupleString);
                                        }
                                );
//                        System.out.println(tupleResult);
                    }
                }

            }
            else if(statement instanceof CreateTable) {
                //Create Table handling
                Evaluator.createTable((CreateTable)statement);
            }
            System.out.print("$>");
        }

    }

}
