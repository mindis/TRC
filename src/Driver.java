import java.util.HashSet;


public class Driver {
	private HashSet<String> tableSet; // current tables
	private int nrPages; // # of pages for each row/column page buffer
	// row pages
	// col pages
	public Driver(int nrPages, String[] tableArray) {
		initPages(nrPages);
		readTables(tableArray);
		processOneScript();
	}
	private void readTables(String[] tableArray) {
		// index starts from 1
		tableSet = new HashSet<String>();
		for (int i = 1; i < tableArray.length; i++) {
			// suppose file name is "table.txt"
			String tableName = tableArray[i].split(".")[0];
			if (tableSet.contains(tableName)) {
				System.err.println("Table " + tableName + " is already existing.");
				continue;
			}
			Operation.createTable(tableName);
			tableSet.add(tableName);
			// continue loading records in this table
		}
	}
	private void initPages(int nrPages) {
		this.nrPages = nrPages / 2;
		// init row pages
		// init col pages
	}


	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: <#pages in memory> [table1, table2, ...]");
			return;
		}
		int nrPages = Integer.parseInt(args[0]);
		if (nrPages < 8 || nrPages % 16 != 0) {
			System.err.println("<#pages> should be an integer of 16k, k = 1, 2, 3...");
			return;
		}
		Driver inst = new Driver(nrPages, args);
	}

}
