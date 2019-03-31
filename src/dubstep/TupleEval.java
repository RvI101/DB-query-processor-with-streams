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
        try {
            return Objects.requireNonNull(currentTuple.stream()
                    .filter(cell -> {
                        String cellColAlias = cell.getAlias().split("\\.")[1];
                        return column.getWholeColumnName().equals(cell.getAlias()) || column.getWholeColumnName().equals(cellColAlias);
                    })
                    .findFirst()
                    .orElse(null))
                    .getValue();
        }
        catch (NullPointerException e) {
            throw new SQLException("404");
        }
    }
}
