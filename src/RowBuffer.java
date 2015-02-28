import java.util.HashMap;
import java.util.List;

public class RowBuffer {
	RowPage[] pageList = null;
	HashMap<String, List<Integer>> table_mm_row_map;
	PageTable pageTable;

	public RowBuffer(int nrPages,
			 PageTable pageTable) {
		pageList = new RowPage[nrPages];
		this.table_mm_row_map = new HashMap<String, List<Integer>>();
		this.pageTable = pageTable;
	}
	
	public List<Integer> getPageList(String tableName){
		if(!table_mm_row_map.containsKey(tableName)) return null;
		return table_mm_row_map.get(tableName);
	}

	class RowPage {
		Tuple[] tuples = null;
		int tupleNum = 0;//the actual tuple number stored so far
		private static final int TUPLE_NUM_PER_PAGE = 512 / 32;

		public RowPage() {
			tuples = new Tuple[TUPLE_NUM_PER_PAGE];
		}

		public Tuple getTuple(int id) {
			for (Tuple tuple : tuples)
				if (tuple.containsId(id))
					return tuple;
			return null;
		}

		public boolean isFull() {
			return tupleNum >= TUPLE_NUM_PER_PAGE;
		}

		public void insertTuple(Tuple tuple) {
			if(!isFull())
				tuples[tupleNum++] = tuple;
		}

	}

	public Tuple getTuple(String tableName, int id) {
		List<Integer> pages = this.getPageList(tableName);
		if(pages == null) return null;
		for (Integer pageId : pages) {
			Tuple tuple = this.pageList[pageId].getTuple(id);
			if (tuple != null)
				return tuple;
		}
		return null;
	}

	public boolean insertTuple(String tableName, Tuple tuple) {
		List<Integer> pages = this.getPageList(tableName);
		for(Integer pageId: pages){
			RowPage page = pageList[pageId];
			if(!page.isFull()){
				page.insertTuple(tuple);
				return true;
			}
		}
		return false;
	}

}
