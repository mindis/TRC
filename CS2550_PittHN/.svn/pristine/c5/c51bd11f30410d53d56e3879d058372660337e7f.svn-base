package Common;


public class Instruction implements Comparable{
	public String transactionID = null;
	public int fileID = -1;;
	public int tidInFile = -1;;
	public String inst = null;
	public String command = null;
	public String table = null;
	public String data = null;
	
	
	
	public Instruction(String cmd){
		this.command = cmd;
		//this.addTimestamp();
	}
	
	public Instruction(String transactionID, String cmd){
		this.transactionID = transactionID;
		this.command = cmd;
		//this.addTimestamp();
	}
	
	public Instruction(int fileID, int tid, String inst, String cmd, String table, String data){
		this.fileID = fileID;
		this.tidInFile = tid;
		this.transactionID = String.valueOf(fileID)+"-"+String.valueOf(tid);
		this.inst = inst;
		this.command = cmd;
		this.table = table;
		this.data = data;
		//this.addTimestamp();
	}
	public Instruction() {
		// TODO Auto-generated constructor stub
	}

	public boolean isReadCommand(){
		return command.equalsIgnoreCase("R");
	
	}

	public boolean isWriteCommand(){
		return command.equalsIgnoreCase("W");
	}

	public boolean isCommitCommand(){
		return command.equalsIgnoreCase("C");
	}

	public boolean isAbortCommand(){
		return command.equalsIgnoreCase("A");
	}


	public boolean isBeginCommand(){
		return command.equalsIgnoreCase("B");
	}


	public boolean isDeleteCommand(){
		return command.equalsIgnoreCase("D");
	}

	public boolean isMatchCommand(){
		return command.equalsIgnoreCase("M");
	}
	public boolean isGCommand(){
		return command.equalsIgnoreCase("G");
	}

	@Override
	public int compareTo(Object o) {
		// TODO should compare "timestamp filed"
		return (int)(this.timestamp-((Instruction)o).timestamp);
	}
public long timestamp;
	
	public static long timestampCounter=0;
	
	public void addTimestamp(){
		this.timestamp=timestampCounter;
		timestampCounter++;
		
	}
	public void setTimeStampCounter(){
		timestampCounter=0;
	}
}
