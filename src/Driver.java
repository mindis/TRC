import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Driver {
	/**
	 * initialize the data structure for TRC, including the hash table, memory ,
	 * page table
	 */
	private static void initResource() {
		ResMgr.initMemPages();
		ResMgr.initHashTable();
		ResMgr.initPageTable();
	}

	/**
	 * reading user operation one line at a time and process the operation by
	 * calling the corresponding operation
	 */
	private static void run() {
		System.out.println("Enter your operation: ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String operation = null;
		while (true) {
			try {
				operation = br.readLine();
			} catch (IOException ioe) {
				System.out.println("IO error trying to read your operation!");
				System.exit(1);
			}
			if (operation == null)
				break;
			else if (!DataManager.isValidOpr(operation)) {
				System.out.printf("operation %s isn't supported yet\n",
						operation);
				continue;
			}
			// valid operation
			System.out.printf("operation %s \n", operation);
			DataManager.execute(operation);
		}
	}

	public static void main(String[] args) {
		CliParser.parse(args);
		initResource();
		run();
	}

}
