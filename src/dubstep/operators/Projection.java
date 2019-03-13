package dubstep.operators;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;

public class Projection extends Operator {
    List<Expression> items;

    public Projection(List<Expression> items) {
        this.items = items;
    }

    public List<Expression> getItems() {
        return items;
    }

    public void setItems(List<Expression> items) {
        this.items = items;
    }
}
