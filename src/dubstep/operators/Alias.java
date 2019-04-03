package dubstep.operators;

import dubstep.Cell;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Alias extends Operator {
    private String alias;
    private Operator child;

    public Alias(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public Stream<List<Cell>> evaluate(Stream<List<Cell>> tupleStream) {
        return tupleStream.map(tuple -> tuple.stream().map(c -> {c.setTable(alias); return c;}).collect(Collectors.toList()));
    }
}
