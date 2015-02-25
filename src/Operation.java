import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;

public class Operation {
	private static final HashSet<String> attrSet = new HashSet<String>() {
		{add("ID"); add("ClientName"); add("Phone");}
	};
	private static final int nrHashBucket = 16;
	public static boolean createTable(String tableName) {
		BinaryStdOut binaryOut;
		for (String attr : attrSet) {
			try {
				binaryOut = new BinaryStdOut(new PrintStream(new FileOutputStream(tableName + "_" + attr)));
				for (int i = 0; i < nrHashBucket; i++)
					binaryOut.write(-1);
				binaryOut.close();
			} catch (IOException e) {
				System.err.println("Error createing/editing file " + tableName + "_" + attr);
				return false;
			}
		}
		return true;
	}
	public static boolean dropTable(String tableName) {
		// also need to delete pages in memory. not implemented yet
		for (String attr : attrSet) {
			File file = new File(tableName + "_" + attr);
			if (!file.delete()) return false;
		}
		return true;
	}
	public static void main(String[] args) {
		System.out.println(createTable("Hello"));
		System.out.println(dropTable("Hello"));
	}
}

