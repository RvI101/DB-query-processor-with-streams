package dubstep.operators;

import dubstep.Cell;
import dubstep.Evaluator;
import net.sf.jsqlparser.schema.Table;

import java.util.List;
import java.util.Objects;
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
        return Objects.requireNonNull(first)
                .flatMap(tuple -> second.map(t -> {t.addAll(tuple); return t;}));
    }
}
