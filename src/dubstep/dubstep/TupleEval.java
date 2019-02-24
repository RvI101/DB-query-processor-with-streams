package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TupleEval extends Eval {
    public List<Cell> currentTuple;

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        return Objects.requireNonNull(currentTuple.stream()
                .filter(cell -> column.getWholeColumnName().equals(cell.getAlias()))
                .findFirst()
                .orElse(null))
                .getValue();
    }
}
