import java.util.HashMap;
import java.util.List;

public class ResMgr {
	public static int nrPages; // # of pages for each row/column page buffer
	private static final int PAGE_SIZE = 512 * 1024;// 512K per page
	public static byte[] rowBuffer, colBuffer;
	// map from table to main memory
	public static HashMap<String, List<Integer>[]> table_mm_map = null;
	// map from table to disk file
	public static HashMap<String, List<String>> table_file_map = null;
	public static PageTable pageTable = null;

	/**
	 * allocate memory pages for row buffer and column buffer
	 * 
	 */
	public static void initMemPages() {
		if (ResMgr.nrPages < 16 || ResMgr.nrPages % 16 != 0) {
			System.err
					.println("<#pages> should be an integer of 16k, k = 1, 2, 3...");
			System.exit(-1);
		}
		rowBuffer = new byte[nrPages * PAGE_SIZE];
		colBuffer = new byte[nrPages * PAGE_SIZE];
	}

	/**
	 * initialize hash table to map between logical table to the memory page and
	 * disk file
	 */
	public static void initHashTable() {
		table_mm_map = new HashMap<String, List<Integer>[]>();
		table_file_map = new HashMap<String, List<String>>();
	}

	public static void initPageTable() {
		pageTable = new PageTable(nrPages);
	}

}
