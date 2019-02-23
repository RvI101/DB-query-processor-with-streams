package dubstep;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.InputStream;

public class Parser {
    public void parse(InputStream input) throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(input);
        Statement statement;
        while ((statement = parser.Statement()) != null) {
            if(statement instanceof Select) {
                //Select handling
            }
            else if(statement instanceof CreateTable) {
                //Create Table handling

            }
        }
    }
}
