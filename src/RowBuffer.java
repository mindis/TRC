import java.util.HashMap;
import java.util.List;

public class RowBuffer {
	RowPage[] pageList = null;
	HashMap<String, List<Integer>> table_mm_row_map;
	PageTable pageTable;

	public RowBuffer(int nrPages,
			HashMap<String, List<Integer>> table_mm_row_map, PageTable pageTable) {
		pageList = new RowPage[nrPages];
		this.table_mm_row_map = table_mm_row_map;
		this.pageTable = pageTable;
	}

	class RowPage {
		Tuple[] tuples = null;
		private static final int TUPLE_NUM_PER_PAGE = 512 / 32;

		public RowPage() {
			tuples = new Tuple[TUPLE_NUM_PER_PAGE];
		}

		public boolean hasTuple(int id) {
			for (Tuple tuple : tuples)
				if (tuple.containsId(id))
					return true;
			return false;
		}
	}

	public boolean hasTuple(String tableName, int id) {
		for (Integer pageId : table_mm_row_map.get(tableName)) {
			if (pageList[pageId].hasTuple(id))
				return true;
		}
		return false;
	}
}
