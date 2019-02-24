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
    private static Map<String, String> aliasMap = new HashMap<>();

    public static void createTable(CreateTable createStatement) {
        tableSchema.put(createStatement.getTable().getAlias() != null
                ? createStatement.getTable().getAlias(): createStatement.getTable().getName(),
                createStatement.getColumnDefinitions()
                .stream()
                .collect(Collectors.toMap(ColumnDefinition::getColumnName,
                        c -> c.getColDataType().getDataType(),
                        (v1,v2) -> {throw new IllegalStateException("Duplicate column in schema");},
                        LinkedHashMap::new)));
    }


    public static boolean doSelect(List<Cell> tuple, Expression expression) {
        TupleEval tupleEval = getEval(tuple);

        try {
            return (tupleEval.eval(expression).toBool());
        } catch (SQLException e) {
            System.out.println("Error doing select");
            return false;
        }
    }

    public static List<Cell> doProject(List<Cell> tuple, List<SelectItem> selectItems){
        TupleEval tupleEval = getEval(tuple);
        if(selectItems.get(0) instanceof AllTableColumns) {
            return tuple;   //Global Wildcard, can return as we know it is the only SelectItem
        }
        List<Cell> projectedTuple = new ArrayList<>();
        for(SelectItem selectItem : selectItems) {
            if(selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                try {
                    projectedTuple.add(new Cell(selectExpressionItem.getAlias(), tupleEval.eval(selectExpressionItem.getExpression())));
                } catch (SQLException e) {
                    System.out.println("Error doing project");
                }
            }
        }
        return projectedTuple;
    }

    private static TupleEval getEval(List<Cell> tuple) {
        TupleEval tupleEval = new TupleEval();
        tupleEval.currentTuple = tuple;
        return tupleEval;
    }

    private static String getTableName(String tableAlias) {
        return aliasMap.getOrDefault(tableAlias, tableAlias);
    }

    private static List<Cell> parseTuple(String tupleString, String tableAlias) {
        Map<String, String> tupleSchema = tableSchema.get(getTableName(tableAlias));
        List<String> typeList = new ArrayList<>(tupleSchema.values());
        List<String> colList = new ArrayList<>(tupleSchema.keySet());
        int i = 0;
        List<Cell> tuple = new ArrayList<>();
        for(String cell : tupleString.split("\\|")) {
            switch (typeList.get(i)) {
                case "string":
                case "varchar":
                case "char":
                    tuple.add(new Cell(tableAlias + "." + colList.get(i), new StringValue(cell)));
                    break;
                case "int":
                    tuple.add(new Cell(tableAlias + "." + colList.get(i), new LongValue(cell)));
                    break;
                case "decimal":
                    tuple.add(new Cell(tableAlias + "." + colList.get(i), new DoubleValue(cell)));
                    break;
                case "date":
                    tuple.add(new Cell(tableAlias + "." + colList.get(i), new DateValue(cell)));
                    break;
                default:
                    System.out.println("Could not parse cell " + cell);
                    tuple.add(new Cell(tableAlias + "." + colList.get(i), new StringValue(cell)));
                    break;
            }
            i++;
        }
        return tuple;
    }

    public static Stream<List<Cell>> tableScan(String tableName, String tableAlias) {
        Path path = Paths.get("/Users/rvi/CSE/DB/team34/data/" + tableName + ".dat");
        if(!tableAlias.equals(tableName) && !aliasMap.containsKey(tableAlias)) {    //Store table aliases
            aliasMap.put(tableAlias, tableName);
        }
        try {
            return Files.lines(path).map(s -> parseTuple(s, tableAlias));
        } catch (IOException e) {
            System.out.println("csv file error " + e.getMessage());
            return null;
        }
    }
}
