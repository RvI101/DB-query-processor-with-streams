package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Evaluator {
    static Map<String, Map<String, String>> tableSchema;
    static Eval eval;

    public static void createTable(CreateTable createStatement) {
        tableSchema.put(createStatement.getTable().getName(), createStatement.getColumnDefinitions()
                .stream()
                .collect(Collectors.toMap(ColumnDefinition::getColumnName, c->c.getColDataType().getDataType())));
    }


    public static boolean doSelect(List<PrimitiveValue> tuple, String schemaName, Expression expression) throws SQLException {
        TupleEval tupleEval = new TupleEval();
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Integer> tupleSchema = tableSchema.get(schemaName).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e->counter.getAndIncrement()));
        tupleEval.tupleSchema = tupleSchema;
        tupleEval.currentTuple = tuple;

        return (tupleEval.eval(expression).toBool());
    }

    public static Stream<String> tableScan(String tableName) {
        Path path = Paths.get("/data/" + tableName + ".csv");
        try {
            return Files.lines(path);
        } catch (IOException e) {
            System.out.println("csv file error " + e.getMessage());
            return null;
        }
    }

}
