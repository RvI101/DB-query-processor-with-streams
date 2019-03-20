package dubstep.operators;

import dubstep.Cell;
import dubstep.TupleEval;

import java.util.List;

public abstract class Operator {
    public Operator() {
    }

    protected static TupleEval getEval(List<Cell> tuple) {
        TupleEval tupleEval = new TupleEval();
        tupleEval.currentTuple = tuple;
        return tupleEval;
    }

    public Operator cascade(Operator o) {
        if(this instanceof Selection) {
            ((Selection) this).setChild(o);
            return o;
        }
        else if(this instanceof Projection) {
            ((Projection) this).setChild(o);
            return o;
        }
        else if(this instanceof Aggregation) {
            ((Aggregation) this).setChild(o);
            return o;
        }
        else if(this instanceof Sort) {
            ((Sort) this).setChild(o);
            return o;
        }
        else if(this instanceof Limit) {
            ((Limit) this).setChild(o);
            return o;
        }
        return null;
    }
}
