package dubstep;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.*;
import java.util.Scanner;

public class Main {

	public static void readFromTable(String tableName) throws IOException {
		InputStream in = new FileInputStream("data/" + tableName + ".csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String content = null;
		while((content=br.readLine())!= null) {
			System.out.println(content);
		}
	}


	public static void main(String[] Args) throws IOException, ParseException {
		Parser.parseAndEvaluate(System.in);
	}
}
