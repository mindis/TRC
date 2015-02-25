package Scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import TransactionManager.*;
import Common.*;
import DataManager.*;


public class Scheduler {
	//TODO
	public static final int OABORT=0;
	public static final int DEADLOCK_ABORT=0;
	
	public static StringBuffer SchedulerLogger = new StringBuffer();
	private Logger log = Logger.getLogger(Scheduler.class.getName());

	static int number_of_commit=0;
	static int number_of_abort=0;
	static int number_of_read=0;
	static int number_of_write=0;
	static int number_of_operation=0;
	static float total_time=0;
	
	
	public LockTable lockTable;
	public TM tranManager;
	public DM dataManager;
	Hashtable<String,Integer> abortList;
	
	ArrayList<Instruction> lockReleasedInstructions;
	
	//Hashtable<Integer, LockTableValue> lock_waiting_tranID_instr;
	Hashtable<String, ArrayList<Instruction>> lock_waiting_tranID_instr;
	
	
	/**
	 * 
	 * @param lockTable
	 * @param tranManager
	 * @param dataManager
	 */
	
public Scheduler(LockTable lockTable, TM tranManager, DM dataManager) {
	
		this.lockTable = lockTable;
		this.tranManager = tranManager;
		this.dataManager=dataManager;
		this.abortList=new Hashtable<String,Integer>();
		this.lock_waiting_tranID_instr=new Hashtable<String, ArrayList<Instruction>>();
		this.lockReleasedInstructions=new ArrayList<Instruction>();
	}

//	TransactionManager tm = new TransactionManager();
	
	//pull a command from transaction manager
	public Instruction pull_operation(){
//		System.out.println("Pull 1 operation from Transaction Manager");
		Instruction command=null;
		

		if(this.hasLockReleaseInstr()){
			command=this.nextLockReleaseInstr();
		}else if(this.tranManager.hasNextInstr()){
			command=this.tranManager.loadNext();
			command.addTimestamp();
		}
		
			
			
			if(command!=null){
				System.out.println("[Scheduler] Pull: TranID:"+command.transactionID+" line_"+command.tidInFile+
										":\t"+command.command+
										" "+command.table+" "+" "+command.data+
										" timestamp:"+ command.timestamp);
				if(command.isReadCommand())
					number_of_read++;
				else if(command.isWriteCommand())
					number_of_write++;
				
				number_of_operation++;
			}
		
		
		return command;
	}
	
	private Instruction nextLockReleaseInstr() {
		// TODO Auto-generated method stub
		if(hasLockReleaseInstr()){
			return this.lockReleasedInstructions.remove(0);
			
		}else{
			return null;
		}
	}

	private boolean hasLockReleaseInstr() {
		// TODO Auto-generated method stub
		
		return this.lockReleasedInstructions!=null&&this.lockReleasedInstructions.size()>0;
	}
	
	private void addLockReleaseInstr(ArrayList<Instruction> waitCMD) {
		// TODO Auto-generated method stub
		this.lockReleasedInstructions.addAll(waitCMD);
		Collections.sort(this.lockReleasedInstructions);
		
		
	}


	//push a command to data manager
	public void push_operation(Instruction cmd){
		log.info("Push 1 operation to Data Manager");
		//push the command to Data Manager to execute
		log.info("TranID="+cmd.transactionID+" type:"+cmd.command
							+" "+cmd.table+" row="+cmd.tidInFile+" timestamp:"+ cmd.timestamp+ "\n");
		
		
		if(cmd.isCommitCommand())
			number_of_commit++;
		if(cmd.isAbortCommand())
			number_of_abort++;
		 
		ArrayList<Instruction> cmdToDM=new ArrayList<Instruction>();
		cmdToDM.add(cmd);
		DM.execute(cmdToDM);
	}
	
	public ArrayList<Instruction> sort_execute_list(ArrayList<Instruction> execute_list){
		
//		Arrays.sort(execute_list);
		Collections.sort(execute_list);
		return execute_list;
	}
	
	public Instruction remove_current_commit_from_commit_list(ArrayList<Instruction> commit_list,Instruction cmd){
		Instruction c=new Instruction();
		for(int i=0; i<commit_list.size(); i++){
			if(commit_list.get(i).transactionID.equals(cmd.transactionID)&&
					commit_list.get(i).fileID==cmd.fileID&&
					commit_list.get(i).tidInFile==cmd.tidInFile){
				c=commit_list.get(i);
				commit_list.remove(i);
				break;
			}
		}
		return c;
	}
	
	public void CommandHandler(Instruction cmd){
		boolean LockObtained=false;
		if(cmd.isBeginCommand()){
			
			this.push_operation(cmd);
			
		} else if(cmd.isReadCommand()||cmd.isWriteCommand()||cmd.isDeleteCommand()||cmd.isMatchCommand()||cmd.isGCommand()){


				
				LockObtained=this.tryToAddLock(cmd);
	
			if(LockObtained==true){//if can obtain lock, should be no deadlock
				System.out.println("[Scheduler] " + "lock obtained");
				this.log.info("[Scheduler] " + "lock obtained");
				push_operation(cmd);
			} else {
				System.out.println("[Scheduler] " + "transaction blocked");
				this.log.info("[Scheduler] " + "transaction blocked");
				this.putLockedInstructions(cmd);
				//TODO put in waitingList
			//TODO deal with deadlock
				String deadLockID=this.lockTable.detectDeadlock();
				if(!deadLockID.equals("")){
					TwoArrayList x=this.lockTable.resolveDeadlock();
					ArrayList<String> toAborts=x.killed;
					ArrayList<String> toExecutes=x.unblocked;
					for(String toabort:toAborts){
						this.putAbotList(toabort, Scheduler.DEADLOCK_ABORT);
						this.log.info("DeadLock Abort: "+toabort);
					}
					
					this.pushAbortInstructionsToDM(toAborts);
					
					for(String toexe:toExecutes){
						this.check_add_lockRelaseInstruction(toexe);
						this.log.info("DeadLock unblock: "+toexe);
					}
					
				}
				
	}
		} else if(cmd.isCommitCommand()) {//*only time that waiting tran can obtain lock is when someone commit
			if(this.check_all_prior_operation_finish(cmd)){
//			if(LM.check_all_prior_operation_finish(cmd,lock_table,lock_waiting_tranID_instr,ComTable,file_lock_table)){
				//LM.ReleaseLock also checked what operation(s) can acquire lock after someone releases lock(s)
				String releaseTranID= this.lockTable.remove(cmd.transactionID);
				push_operation(cmd);
				
				if(!releaseTranID.equals("")){
					this.check_add_lockRelaseInstruction(releaseTranID);

				}
				
			} else {
				this.putLockedInstructions(cmd);

			}
		} else if(cmd.isAbortCommand()){
			//LM.ReleaseLock also checked what operation(s) can acquire lock after someone releases lock(s)
			String releaseTranID= this.lockTable.remove(cmd.transactionID);
			push_operation(cmd);
			this.putAbotList(cmd,Scheduler.DEADLOCK_ABORT);
			
			if(!releaseTranID.equals("")){
				this.check_add_lockRelaseInstruction(releaseTranID);
			}
		} 
	}
	
	private boolean tryToAddLock(Instruction cmd) {
		// TODO Auto-generated method stub
		boolean LockObtained=false;
		if(!hasWaitingCMD(cmd.transactionID)){
			
			if(cmd.isMatchCommand()||cmd.isDeleteCommand()||cmd.isGCommand()){
				
				LockObtained=this.lockTable.add(LockTableElement.CONSTRUCT_LTE(cmd),cmd.table);
			//	System.out.println("****************"+cmd.transactionID);
				
			}else{
				LockObtained=this.lockTable.add(LockTableElement.CONSTRUCT_LTE(cmd), cmd.table,cmd.data);
				System.out.println(cmd.transactionID);
				//System.out.println("****************"+cmd.transactionID);
			}
		}
		
		return LockObtained;
	}



	private void pushAbortInstructionsToDM(ArrayList<String> toAborts) {
		// TODO Auto-generated method stub
		for(String abortTID:toAborts){
			Instruction cmd=new Instruction(abortTID,"a");
			this.push_operation(cmd);
		}
		
	}

	private void putAbotList(Instruction cmd,int whyAbort) {
		// TODO Auto-generated method stub
		this.abortList.put(cmd.transactionID,whyAbort);
		
	}
	
	private void putAbotList(String tranID,int whyAbort) {
		// TODO Auto-generated method stub
		this.abortList.put(tranID,whyAbort);
		
	}

	private void putLockedInstructions(Instruction cmd) {
		// TODO Auto-generated method stub
		ArrayList<Instruction> waitCMD=null;
		if(this.lock_waiting_tranID_instr.containsKey(cmd.transactionID)){
			waitCMD=this.lock_waiting_tranID_instr.get(cmd.transactionID);
			waitCMD.add(cmd);
			this.lock_waiting_tranID_instr.put(cmd.transactionID, waitCMD);
		}else{
			waitCMD=new ArrayList<Instruction>();
			waitCMD.add(cmd);
			this.lock_waiting_tranID_instr.put(cmd.transactionID, waitCMD);
		}
		
		
	}

	private void check_add_lockRelaseInstruction(String releaseTranID) {
		// TODO Auto-generated method stub
		if(this.hasWaitingCMD(releaseTranID)){
			ArrayList<Instruction> waitCMD=this.lock_waiting_tranID_instr.remove(releaseTranID);
			this.addLockReleaseInstr(waitCMD);
			
		}
		
	}


	private boolean hasWaitingCMD(String releaseTranID) {
		// TODO Auto-generated method stub
		return this.lock_waiting_tranID_instr.containsKey(releaseTranID);
	}

	public boolean CheckAbortedList(Instruction cmd){
		if(abortList.get(cmd.transactionID)!=null){
			System.out.println("Aborted!");
			return false;
		}else
			return true;
	}
	
	/**
	 * 
	 * @param cmd
	 * @return
	 */
	public boolean check_all_prior_operation_finish(Instruction cmd) {
		
		if(this.lock_waiting_tranID_instr.containsKey(cmd.transactionID)){
			return false;
		}else{
			return true;
		}

	}
	
	
	
	public void Schedule_Transactions(){
		
		try {
			FileHandler filehandler  = new FileHandler("scheduler.log",true);
			filehandler.setFormatter(new SimpleFormatter());
			log.addHandler(filehandler);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("[Scheduler] STart scheduling transaction");

		float startTime=System.nanoTime();//.currentTimeMillis();
		Instruction command=pull_operation();
		while(command!=null) {
			if(CheckAbortedList(command)==true){//no need to handle all aborted transaction operation
				//commandHandler return which command to execute next
				CommandHandler(command);
			}
			System.out.println("");
			command=pull_operation();
		}
		float endTime=System.nanoTime();//.currentTimeMillis();
		total_time=endTime-startTime;
		log.info("number of commit="+ number_of_commit+ " number_of_abort="+number_of_abort + " number of read="+number_of_read
				+" nnumber_of_write="+number_of_write+" total_number_of_operation="+number_of_operation
				+ " average response time="+total_time/number_of_operation +" nano second");
	}


	public static void main(String[] args){
		
		LockTable lockTable=new LockTable(); 
		DM.tables = "X,Y".split(",");
	//	TM.codes = new String[]{ "test/shortReadTrx.txt" ,"test/shortGeneralTrx.txt","test/shortArrgTrx.txt"};
		TM.codes = new String[]{ "test/script1" ,"test/script2","test/script3"};
		TM tranManager=new TM(); 
		DM dataManager=new DM();
		tranManager.execute();
		Scheduler scheduler=new Scheduler( lockTable,  tranManager,  dataManager);
		scheduler.Schedule_Transactions();
	}

}
