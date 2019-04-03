package dubstep.operators;

import dubstep.Cell;
import dubstep.Parser;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sort extends Operator {
    Operator child;
    List<OrderByElement> columns;
    private final static int BLOCK_SIZE = 500;
    List<Path> tmpPaths = new ArrayList<>();
    private File tmpDirectory;

    public Sort() {
        try {
            tmpDirectory = Files.createDirectories(Paths.get("data/tmp")).toFile();
        } catch (IOException e) {
            System.out.println("Error creating tmp directory");
        }
    }

    public Sort(List<OrderByElement> columns) {
        this();
        this.columns = columns;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public List<OrderByElement> getColumns() {
        return columns;
    }

    public void setColumns(List<OrderByElement> columns) {
        this.columns = columns;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tupleStream) {
        if(Parser.mode == 1)
            return sortInMemory(tupleStream);
        else {
            try {
                return sortOnDisk(tupleStream);
            } catch (IOException e) {
                System.out.println("Error performing 2-pass sort");
                return Stream.empty();
            }
        }
    }

    public Stream<List<Cell>> sortInMemory(Stream<List<Cell>> tupleStream) {
        return tupleStream.sorted(tupleComparator);
    }

    public Stream<List<Cell>> sortOnDisk(Stream<List<Cell>> tupleStream) throws IOException {
        Iterator<List<Cell>> it = tupleStream.iterator();
        List<List<Cell>> block = new ArrayList<>();
        int i = 0;
        while(it.hasNext()) { //Pass 1
            List<Cell> tuple = it.next();
            if(i++ > BLOCK_SIZE) {
                block.sort(tupleComparator);
                writeToDisk(block.stream());
                block.clear();
                i = 0;
            }
            block.add(tuple);
        }
        Map<String, String> tupleSchema = block.get(0)
                .stream()
                .collect(Collectors.toMap(Cell::getWholeName,
                        c -> Parser.typeOf(c.getValue()),
                        (k1,k2) -> {throw new IllegalStateException("Duplicate column in schema");},
                        LinkedHashMap::new));
        block.sort(tupleComparator);
        writeToDisk(block.stream());
        block.clear();
        List<Path> intermediatePaths = new ArrayList<>();
        while(tmpPaths.size() > 1) {
            int j = 0;
            for (j = 0; j <= tmpPaths.size() - 2; j += 2) {
                Stream<List<Cell>> first = Files.lines(tmpPaths.get(j)).map(s -> Parser.parseTuple(s, tupleSchema));
                Stream<List<Cell>> second = Files.lines(tmpPaths.get(j + 1)).map(s -> Parser.parseTuple(s, tupleSchema));
                intermediatePaths.add(mergeToDisk(first, second));
            }
            if (j == tmpPaths.size() - 1) {
                intermediatePaths.add(tmpPaths.get(j));
                j++;
            }
            tmpPaths.clear();
            tmpPaths.addAll(intermediatePaths);
            intermediatePaths.clear();
        }
        Stream<List<Cell>> sortedStream = Files.lines(tmpPaths.get(0)).map(s -> Parser.parseTuple(s, tupleSchema));
        for(File file: tmpDirectory.listFiles()) {
            if (!file.isDirectory())
                file.delete();
        }
        return sortedStream;
    }

    private Path mergeToDisk(Stream<List<Cell>> first, Stream<List<Cell>> second) {
        Iterator<List<Cell>> firstIt = first.iterator();
        Iterator<List<Cell>> secondIt = second.iterator();
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("merge", ".tmp", tmpDirectory);
        } catch (IOException e) {
            System.out.println("Error creating file");
        }
        try {
            List<Cell> firstTuple = firstIt.next();
            List<Cell> secondTuple = secondIt.next();
            do {
                if (tupleComparator.compare(firstTuple, secondTuple) < 0) { //first tuple < second tuple
                    writeTupleToDisk(tmpFile, firstTuple);
                    if(firstIt.hasNext())
                        firstTuple = firstIt.next();
                    else
                        break;
                }
                else {
                    writeTupleToDisk(tmpFile, secondTuple);
                    if(secondIt.hasNext())
                        secondTuple = secondIt.next();
                    else
                        break;
                }
            } while (true);
            while(firstIt.hasNext()) {
                writeTupleToDisk(tmpFile, firstTuple);
                firstTuple = firstIt.next();
            }
            while(secondIt.hasNext()) {
                writeTupleToDisk(tmpFile, secondTuple);
                secondTuple = secondIt.next();
            }
            return tmpFile.toPath().toAbsolutePath();
        } catch (IOException e) {
            System.out.println("Error appending to file " + tmpFile.getName());
            return null;
        } catch (NoSuchElementException e) {
            System.out.println("There are no tuples in this/these stream(s)");
            return tmpFile.toPath().toAbsolutePath();
        }
    }

    private String singleLine(String s) {
        return s + "\n";
    }

    private void writeTupleToDisk(File tmpFile, List<Cell> tuple) throws IOException {
        Files.write(tmpFile.toPath().toAbsolutePath(),
                singleLine(tuple.stream().map(Cell::toString).collect(Collectors.joining("|"))).getBytes(),
                StandardOpenOption.APPEND);
    }

    private void writeToDisk(Stream<List<Cell>> tupleList) {
        try {
            File tmpFile = File.createTempFile("pass1", ".tmp", tmpDirectory);
            tupleList.forEach(t -> {
                                try {
                                    writeTupleToDisk(tmpFile, t);
                                } catch (IOException e) {
                                    System.out.println("Error writing tuple to file");
                                }
                            });
            tmpPaths.add(tmpFile.toPath().toAbsolutePath());
        } catch (IOException e) {
            System.out.println("Error creating file");
        }
    }

    private Comparator<List<Cell>> tupleComparator = new Comparator<List<Cell>>() {
        @Override
        public int compare(List<Cell> a, List<Cell> b) {
            int val = 0;
            for(OrderByElement element : columns) {
                try {
                    Expression expression = element.getExpression();
                    int orderPolarity = element.isAsc() ? 1 : -1;
                    PrimitiveValue first = getEval(a).eval(expression);
                    PrimitiveValue second = getEval(b).eval(expression);
                    switch(first.getType()) {
                        case LONG:
                            val = Long.compare(first.toLong(), second.toLong()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case DOUBLE:
                            val = Double.compare(first.toDouble(), second.toDouble()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case STRING:
                            val = first.toString().compareTo(second.toString()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case BOOL:
                            val = Boolean.compare(first.toBool(),second.toBool()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case DATE:
                            val = ((DateValue)first).getValue().compareTo(((DateValue)second).getValue()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case TIMESTAMP:
                            val = ((TimestampValue)first).getValue().compareTo(((TimestampValue)second).getValue()) * orderPolarity;
                            if(val == 0)
                                continue;
                            return val;
                        case TIME:
                            val = ((TimeValue)first).getValue().compareTo(((TimeValue)second).getValue()) * orderPolarity;
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
}
