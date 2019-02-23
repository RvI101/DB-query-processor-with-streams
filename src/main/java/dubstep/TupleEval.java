package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class TupleEval extends Eval {
    public Map<String, Integer> tupleSchema;
    public List<PrimitiveValue> currentTuple;

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        int colID = tupleSchema.get(column.getColumnName());
        return currentTuple.get(colID);
    }
}
