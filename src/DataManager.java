public class DataManager {

	public static void execute(String opr) {
		String[] tokens = opr.split("\\s");
		if (tokens[0].equals("R")) {
			byte[] tuple = retrieve(tokens[1], Integer.parseInt(tokens[2]));
			if (tuple != null)
				printTuple(tuple);
			else
				System.out.println("no such id exist in the table");
		} else if (tokens[0].equals("I"))
			insert(tokens[1], tokens[2]);
	}

	public static boolean isValidOpr(String operation) {
		// TODO Auto-generated method stub
		return true;
	}

	private static byte[] retrieve(String tableName, int id) {
		// check if the table exist or not
		if (!ResMgr.containsTable(tableName)) {
			System.out.println("table doesn't exist!");
			return null;
		}
		// check if the the tuple containing the id is in the rowBuffer
		byte[] tuple = ResMgr.findTupleInRowBuffer(id);
		if (tuple != null)
			return tuple;
		// if no tuple in the rowbuffer, bring pages from disk file into the
		// rowbuffer
		ResMgr.movePagesFromFileToRowBuffer(id);
		tuple = ResMgr.findTupleInRowBuffer(id);
		return tuple;
	}

	private static void insert(String tableName, String tupleStr) {
		Tuple tuple = new Tuple(tupleStr);
		// if table doesn't exist, create physical disk file and no mem page.
		//later, retrieve will find mem page and establish mapping
		if (!ResMgr.containsTable(tableName)) {
			ResMgr.createFilesForTable(tableName);
			ResMgr.insertTupleToNewFilePage(tableName, tuple);
			return;
		}
		// if the table exists, if mem_page contains the tuple to be
		// inserted, error
		int id = tuple.getId();
		if(ResMgr.hasTupleInRowBuffer(tableName, id)){
			System.err.println("tuple already exists in the row buffer, error!");
			System.exit(-1);
		}
		//insert the tuple to the disk file and not to mem page.
		//later, retrieve will find mem page and establish mapping
		
		// tuple doesn't exist, insert the tuple into the mem_page.
		// the tuple would be swapped out to the file later by LRU
		ResMgr.insertTupleIntoRowBuffer(tuple);
	}

	private static void printTuple(byte[] tuple) {
		// TODO Auto-generated method stub

	}
}
