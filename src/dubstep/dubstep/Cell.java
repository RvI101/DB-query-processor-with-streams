package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class Cell {
    String alias;
    PrimitiveValue value;

    public Cell(String alias, PrimitiveValue value) {
        this.alias = alias;
        this.value = value;
    }

    public Cell() {
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public PrimitiveValue getValue() {
        return value;
    }

    public void setValue(PrimitiveValue value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toRawString();
    }
}
