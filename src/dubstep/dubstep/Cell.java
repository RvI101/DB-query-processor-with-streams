package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class Cell {
    String table;
    String alias;
    PrimitiveValue value;

    public Cell(String alias, PrimitiveValue value) {
        this.alias = alias;
        this.value = value;
        this.table = alias.split("\\.")[0] != null ? alias.split("\\.")[0] : "";
    }

    public Cell() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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
