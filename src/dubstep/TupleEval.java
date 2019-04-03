package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class TupleEval extends Eval {
    public List<Cell> currentTuple;

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        try {
            return Objects.requireNonNull(currentTuple.stream()
                    .filter(cell -> {
                        String cellColAlias = cell.getWholeName();
                        return column.getWholeColumnName().equals(cellColAlias) || column.getWholeColumnName().equals(cell.getName());
                    })
                    .findFirst()
                    .orElse(null))
                    .getValue();
        }
        catch (NullPointerException e) {
            throw new SQLException("404");
        }
    }

    @Override
    public PrimitiveValue eval(Function function) throws SQLException {
        String fn = function.getName().toUpperCase();
        if ("DATE".equals(fn)) {
            List args = function.getParameters().getExpressions();
            if (args.size() != 1) {
                throw new SQLException("DATE() takes exactly one argument");
            } else {
                return new DateValue(this.eval((Expression) args.get(0)).toRawString());
            }
        } else {
            return eval(new Column(new Table(), function.toString())); //We assume the function result has already been projected as a column
        }
    }
}
