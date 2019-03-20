package dubstep.operators;

import dubstep.Cell;

import java.util.List;
import java.util.stream.Stream;

public class Limit extends Operator{
    Operator child;
    private long num;

    public Limit(long num) {
        this.num = num;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tuples) {
        return tuples.limit(num);
    }
}
