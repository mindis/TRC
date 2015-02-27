import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;

public class ResMgr {
	public static int nrPages; // # of pages for each row/column page buffer
	private static final int PAGE_SIZE = 512;// 512 bytes per page
	private static final int TUPLE_SIZE = 32; // 32 bytes per tuple
	public static byte[] rowBuffer, colBuffer;
	// map from table to main memory
	public static HashMap<String, List<Integer>[]> table_mm_map = null;
	// map from table to disk file
	public static HashMap<String, String[]> table_file_map = null;
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
		table_file_map = new HashMap<String, String[]>();
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

	public static void movePagesFromFileToRowBuffer(int id) {
		// TODO Auto-generated method stub

	}

	public static boolean containsTable(String tableName) {
		// TODO Auto-generated method stub
		return table_file_map.containsKey(tableName);
	}

	public static void createFilesForTable(String tableName) {
		String idFileName = tableName + "_id", nameFileName = tableName
				+ "_name", phoneFileName = tableName + "_phone";
		String[] files = new String[] { idFileName, nameFileName, phoneFileName };
		for (String file : files)
			createEmptyFile(file);
		// update the table_file_map
		table_file_map.put(tableName, files);
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
	 * insert each element of the tuple into the corresponding files at the unit
	 * of Page
	 * 
	 * @param tableName
	 * @param tuple
	 */
	public static void insertTupleToFiles(String tableName, Tuple tuple) {
		int id = tuple.getId();
		int bucketNum = getHashBucket(id);
		String[] fileNames = table_file_map.get(tableName);
		for(String fileName: fileNames){
			try {
				byte[] header = readFromFile(fileName, 0, 4 * 16);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("invalid header!");
				e.printStackTrace();
			}
			for(int i = 0; i < 16; i++){
				byte[] 
			}
		}

	}

	public static void insertTupleIntoRowBuffer(String tupleStr) {
		// TODO Auto-generated method stub

	}

	private static int getHashBucket(int id) {
		return id % 16;
	}

	private static byte[] readFromFile(String filePath, int position, int size)
			throws IOException {

		RandomAccessFile file = new RandomAccessFile(filePath, "r");
		file.seek(position);
		byte[] bytes = new byte[size];
		file.read(bytes);
		file.close();
		return bytes;

	}

	private static void writeToFile(String filePath, byte[] data, int position)
			throws IOException {

		RandomAccessFile file = new RandomAccessFile(filePath, "rw");
		file.seek(position);
		file.write(data);
		file.close();

	}

}
