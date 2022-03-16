import java.io.FileWriter;
import java.io.IOException;

public class TestCode {
	
	public TestCode() {
		
	}
	public static void init() throws IOException {
		FileWriter file = new FileWriter("metadata.csv");
		file.write("Table Name,");
		file.write("Column Name,");
		file.write("Column Type,");
		file.write("ClusteringKey,");
		file.write("Indexed,");
		file.write("min,");
		file.write("max");
		
		file.flush();
		file.close();

	}
	public static void main(String[] args) throws IOException {
		TestCode t = new TestCode();
		t.init();
		
	}

}
