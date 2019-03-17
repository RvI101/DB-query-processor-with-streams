package dubstep.operators;

import dubstep.Cell;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class TableScan extends Operator {
    String tableName;
    String tableAlias;

    private static Map<String, LinkedHashMap<String, String>> tableSchema = new LinkedHashMap<>();
    private static Map<String, String> aliasMap = new HashMap<>();

    public TableScan() {
    }

    public TableScan(String tableName, String tableAlias) {
        this.tableName = tableName;
        this.tableAlias = tableAlias;
    }

    public TableScan(String tableName) {
        this.tableName = tableName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Stream<List<Cell>> tableScan() {
        Path path = Paths.get("data/" + tableName + ".csv");
        if(!tableAlias.equals(tableName) && !aliasMap.containsKey(tableAlias)) {    //Store table aliases
            aliasMap.put(tableAlias, tableName);
        }
        try {
            return Files.lines(path).parallel().map(s -> parseTuple(s, tableAlias));
        } catch (IOException e) {
            System.out.println("csv file error " + e.getMessage());
            return null;
        }
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
}
