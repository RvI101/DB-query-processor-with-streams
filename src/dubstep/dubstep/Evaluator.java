package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Evaluator {
    private static Map<String, LinkedHashMap<String, String>> tableSchema = new LinkedHashMap<>();
    static Eval eval;

    public static void createTable(CreateTable createStatement) {
        tableSchema.put(createStatement.getTable().getName(), createStatement.getColumnDefinitions()
                .stream()
                .collect(Collectors.toMap(ColumnDefinition::getColumnName,
                        c -> c.getColDataType().getDataType(),
                        (v1,v2) -> {throw new IllegalStateException("Duplicate column in schema");},
                        LinkedHashMap::new)));
    }


    public static boolean doSelect(List<PrimitiveValue> tuple, String schemaName, Expression expression) {
        TupleEval tupleEval = getEval(schemaName, tuple);

        try {
            return (tupleEval.eval(expression).toBool());
        } catch (SQLException e) {
            System.out.println("Error doing select");
            return false;
        }
    }

    public static List<PrimitiveValue> doProject(List<PrimitiveValue> tuple, String schemaName, List<SelectItem> selectItems){
        TupleEval tupleEval = getEval(schemaName, tuple);
        if(selectItems.stream().anyMatch(selectItem -> (selectItem instanceof AllTableColumns))) {
            return tuple;   //Global Wildcard, can return as we know it is the only SelectItem
        }
        List<PrimitiveValue> projectedTuple = new ArrayList<>();
        for(SelectItem selectItem : selectItems) {
            if(selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                try {
                    projectedTuple.add(tupleEval.eval(selectExpressionItem.getExpression()));
                } catch (SQLException e) {
                    System.out.println("Error doing project");
                }
            }
        }
        return projectedTuple;
    }

//    public static Stream<String> simpleJoin()

    private static TupleEval getEval(String schemaName, List<PrimitiveValue> tuple) {
        TupleEval tupleEval = new TupleEval();
        AtomicInteger counter = new AtomicInteger(0);
        tupleEval.tupleSchema = tableSchema.get(schemaName).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e->counter.getAndIncrement()));
        tupleEval.currentTuple = tuple;

        return tupleEval;
    }

    public static List<PrimitiveValue> parseTuple(String tupleString, String tablename) {
        Map<String, String> tupleSchema = tableSchema.get(tablename);
        List<String> typeList = new ArrayList<>(tupleSchema.values());
        int i = 0;
        List<PrimitiveValue> tuple = new ArrayList<>();
        for(String cell : tupleString.split("\\|")) {
            switch (typeList.get(i)) {
                case "string":
                case "varchar":
                case "char":
                    tuple.add(new StringValue(cell));
                    break;
                case "int":
                    tuple.add(new LongValue(cell));
                    break;
                case "decimal":
                    tuple.add(new DoubleValue(cell));
                    break;
                case "date":
                    tuple.add(new DateValue(cell));
                    break;
                default:
                    System.out.println("Could not parse cell " + cell);
                    tuple.add(new StringValue(cell));
                    break;
            }
            i++;
        }
        return tuple;
    }

    public static Stream<List<PrimitiveValue>> tableScan(String tableName) {
        Path path = Paths.get("/Users/rvi/CSE/DB/team34/data/" + tableName + ".dat");
        try {
            return Files.lines(path).map(s -> parseTuple(s, tableName));
        } catch (IOException e) {
            System.out.println("csv file error " + e.getMessage());
            return null;
        }
    }
}
