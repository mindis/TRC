public class DataManager {

	public static void execute(String opr) {
		String[] tokens = opr.split("\\s");
		if (tokens[0].equals("R")) {
			Tuple tuple = retrieve(tokens[1], Integer.parseInt(tokens[2]));
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


	/**
	 * 1) search the row buffer sequentially to see if the tuple is there 2) if
	 * not, bring disk file to the row buffer 3) search the row buffer again.
	 * 
	 * @param tableName
	 * @param id
	 * @return
	 */

	private static Tuple retrieve(String tableName, int id) {
		Tuple tuple = ResMgr.getTuple(tableName, id);
		if (tuple != null)
			return tuple;
		// if no tuple in the rowbuffer, bring pages from disk file into the
		// rowbuffer
		// TODO
		ResMgr.movePageFromFileToRowBuffer(tableName, id);
		tuple = ResMgr.getTuple(tableName, id);
		if (tuple != null)
			return tuple;
		System.err.println("no such tuple exist in the table");
		return null;
	}

	private static void insert(String tableName, String tupleStr) {
		Tuple tuple = new Tuple(tupleStr);
		int id = tuple.getId();
		// if table doesn't exist, create physical disk file and no mem page.
		// later, retrieve will find mem page and establish mapping
		if (!ResMgr.containsTable(tableName)) {
			ResMgr.createFilesForTable(tableName);
			ResMgr.insertTupleToNewFilePage(tableName, tuple);
			return;
		}
		// if the table exists, if mem_page contains the tuple to be
		// inserted, error
		if (retrieve(tableName, id) != null) {
			System.err
					.println("tuple already exists in the row buffer, error!");
			System.exit(-1);
		}
		// tuple doesn't exist, insert the tuple into the mem_page.
		// the tuple would be swapped out to the file later by LRU
		ResMgr.insertTupleIntoRowBuffer(tableName, tuple);
	}

	private static void printTuple(Tuple tuple) {
		System.out.println("Tuple: " + tuple.toString());
	}
}
