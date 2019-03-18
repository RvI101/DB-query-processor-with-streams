package dubstep.operators;

import dubstep.Cell;
import dubstep.Evaluator;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrossProduct extends Operator {
    Operator left;
    Operator right;

    public CrossProduct(Operator left, Operator right) {
        this.left = left;
        this.right = right;
    }

    public CrossProduct() {
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> first, Stream<List<Cell>> second) {
        List<List<Cell>> secondTable = second.collect(Collectors.toList()); // storing second table's tuples in memory for cross product
        return Objects.requireNonNull(first)
                .flatMap(tuple -> secondTable.stream()
                        .map(t -> concatenate(tuple, t)));
    }

    private List<Cell> concatenate(List<Cell> first, List<Cell> second) {
        List<Cell> result = new ArrayList<>();
        result.addAll(first);
        result.addAll(second);
        return result;
    }
}
