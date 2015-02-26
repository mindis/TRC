
public class DataManager {
	public DataManager(){
		
	}
	
	public static void execute(String opr){
	    String[] tokens = opr.split("\\s");
	    if(tokens[0].equals("R")) retrieve(tokens[1], Integer.parseInt(tokens[2]));
	    else if(tokens[0].equals("I")) insert(tokens[1], tokens[2]);
	}

	

	public static boolean isValidOpr(String operation) {
		// TODO Auto-generated method stub
		return true;
	}
	
	private static void retrieve(String tableName, int id){
		//check if the table exist or not 
		if(!ResMgr.table_file_map.containsKey(tableName)){
			System.out.println("table doesn't exist!");
			return;
		}
	}
	
	private static void insert(String tableName, String tuple) {
		// TODO Auto-generated method stub
		
	}
}
