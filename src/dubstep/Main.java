package dubstep;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Main {
	
	public static void readFromTable(String tableName) throws IOException {
		 InputStream in = new FileInputStream("data/" + tableName + ".csv");
		 BufferedReader br = new BufferedReader(new InputStreamReader(in));
		 String content = null;
		 while((content=br.readLine())!= null) {
			 System.out.println(content);
		 }
	}
	
	
	public static void main(String[] Args) throws ParseException, IOException {
		
		 Scanner s = new Scanner(System.in);
		 String userInput = s.nextLine();
		
		 StringReader input = new StringReader(userInput);
		 CCJSqlParser parser = new CCJSqlParser(input);
		 Statement query = parser.Statement();
		 
		 if(query instanceof Select) {
			 Select select = (Select) query;
			 if(select.getSelectBody() instanceof PlainSelect) {
				 PlainSelect ps = (PlainSelect) select.getSelectBody();
				 FromItem item = ps.getFromItem();
				 if(item instanceof Table) {
					 Table table = (Table) item;
					 String tableName = ((Table) table).getName();
					 readFromTable(tableName);
				 }
			 }
			
		 }
		 
		
				 
	}
	
}
