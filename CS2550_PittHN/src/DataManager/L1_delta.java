package DataManager;

import java.util.ArrayList;
import java.util.HashMap;

public class L1_delta {
	
	public static HashMap<String, ArrayList> tablePages = new HashMap<String, ArrayList>(); 
	
	public static int[] pageAvailability = new int[DM.L1_SIZE];// 0:empty, 1: not full, 2:full
	
	public static Page[] buffer = new Page[DM.L1_SIZE];
	
	public static void clear(){
		tablePages = new HashMap<String, ArrayList>();
		for(int i=0;i<DM.tables.length;i++){
			 L1_delta.tablePages.put(DM.tables[i], new ArrayList());
		}
		pageAvailability = new int[DM.L1_SIZE];
		buffer = new Page[DM.L1_SIZE];
	}
}
