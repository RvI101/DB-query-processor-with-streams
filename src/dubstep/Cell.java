package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class Cell {
    private String table;
    private String name;
    private PrimitiveValue value;
    private String alias;

    public Cell(String name, PrimitiveValue value) {
        this.value = value;
        this.table = name.contains(".") ? name.split("\\.")[0] : null;
        this.name = name.contains(".") ? name.split("\\.")[1]: name;
    }

    public Cell(String name, PrimitiveValue value, String alias) {
        this.table = name.contains(".") ? name.split("\\.")[0] : null;
        this.name = name.contains(".") ? name.split("\\.")[1]: name;
        this.value = value;
        this.alias = alias;
    }

    public Cell() {
    }

    public String getWholeName() {
       if(alias != null)
           return alias;
       else if(table != null)
           return table + "." + name;
       else
           return name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
