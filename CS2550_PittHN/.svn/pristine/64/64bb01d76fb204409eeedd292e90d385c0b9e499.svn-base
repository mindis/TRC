package DataManager;

public class DMLog {
	public static int LogID=0;

	String transactionID = null;;
	String operation = null;
	String table = null;
	String data = null;
	

	long timeOfRequestReceived = 0;
	
	long timeOfRequestFinished = 0;
	
	public DMLog(String tID, String op, String table, String data){
		this.transactionID = tID;
		this.operation= op;
		this.table = table;
		this.data = data;
	
		this.timeOfRequestReceived = System.nanoTime();
		LogID++;
	}

	
	public void setFinishtTimestamp(){
		this.timeOfRequestFinished = System.nanoTime();
	}
}

