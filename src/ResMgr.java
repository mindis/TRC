import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ResMgr {
	public static int nrPages; // # of pages for each row/column page buffer
	private static final int PAGE_SIZE = 512;// 512 bytes per page
	private static final int TUPLE_SIZE = 32; // 32 bytes per tuple
	public static RowBuffer rowBuffer;
	public static byte[] colBuffer;
	// map from table to main memory
	public static HashMap<String, List<Integer>> table_mm_row_map = null,
			table_mm_col_map = null;
	// map from table to disk file
	public static HashMap<String, DiskFile[]> table_file_map = null;
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
		rowBuffer = new RowBuffer(nrPages, table_mm_row_map, pageTable);
		colBuffer = new byte[nrPages * PAGE_SIZE];
	}

	/**
	 * initialize hash table to map between logical table to the memory page and
	 * disk file
	 */
	public static void initHashTable() {
		table_mm_row_map = new HashMap<String, List<Integer>>();
		table_mm_col_map = new HashMap<String, List<Integer>>();
		table_file_map = new HashMap<String, DiskFile[]>();
	}

	public static void initPageTable() {
		pageTable = new PageTable(nrPages);
	}

	/**
	 * Search the row buffer to find the tuple with the corresponding id
	 * 
	 * @param id
	 * @return
	 */
	public static byte[] findTupleInRowBuffer(int id) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void movePageFromFileToRowBuffer(int id) {
		// TODO Auto-generated method stub

	}

	public static boolean containsTable(String tableName) {
		return table_file_map.containsKey(tableName);
	}

	public static void createFilesForTable(String tableName) {
		String idFileName = tableName + "_id", nameFileName = tableName
				+ "_name", phoneFileName = tableName + "_phone";
		String[] fileNames = new String[] { idFileName, nameFileName,
				phoneFileName };
		for (String fileName : fileNames)
			createEmptyFile(fileName);
		DiskFile[] files = new DiskFile[] { new DiskFile(fileNames[0]),
				new DiskFile(fileNames[1]), new DiskFile(fileNames[2]) };
		// update the table_file_map
		table_file_map.put(tableName, files);
		table_mm_row_map.put(tableName, new ArrayList<Integer>());
		table_mm_col_map.put(tableName, new ArrayList<Integer>());
	}

	private static void createEmptyFile(String fileName) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
			for (int i = 0; i < 16; i++)
				bw.write(-1);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Under this two situation this function gets called:
	 * 1) when no file exist for the table
	 * 2) when the tuple to be inserted cannot fit into memory page, we should create new disk
	 * page in order to hold the element of the new tuple
	 * 
	 * there's nothing to do with the page table, page table is updated only when retrieve happened.
	 * When insertion stop at the memory level, just set the dirty bit of the page table so that
	 * when swapped out, the content will be write back to the memory.
	 * 
	 * @param tableName
	 * @param tuple
	 */
	public static void insertTupleToNewFilePage(String tableName, Tuple tuple) {
		// TODO: update bucket pointers-->fseek()
		DiskFile[] diskFiles = table_file_map.get(tableName);
		DiskFile idFile = diskFiles[0], nameFile = diskFiles[1], phoneFile = diskFiles[2];
		idFile.createNewPageToHoldTupleId(tuple);
		nameFile.createNewPageToHoldTupleName(tuple);
		phoneFile.createNewPageToHoldTuplePhone(tuple);
	}

	public static void insertTupleIntoRowBuffer(Tuple tuple) {
		// TODO Auto-generated method stub

	}

	public static boolean hasTupleInRowBuffer(String tableName, int id) {
		// TODO Auto-generated method stub
		return rowBuffer.hasTuple(tableName, id);
	}

}
