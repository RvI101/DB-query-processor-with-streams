package dubstep;
import net.sf.jsqlparser.parser.ParseException;

import java.io.*;

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
		Parser.parseAndEvaluate();
	}
}
