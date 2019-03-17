package dubstep.operators;

import dubstep.Cell;

import java.util.List;
import java.util.stream.Stream;

public class Union extends Operator {
    Operator left;
    Operator right;

    public Union() {
    }

    public Union(Operator left, Operator right) {
        this.left = left;
        this.right = right;
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
        return  Stream.concat(first, second);
    }
}
