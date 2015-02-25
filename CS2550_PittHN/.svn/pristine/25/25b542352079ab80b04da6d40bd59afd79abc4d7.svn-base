import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import TransactionManager.*;
import DataManager.*;
import Scheduler.*;

public class Main {
	public static void main(String[] args) {
		
		System.out.println("*** PittHN starts executing ***");
		
		
		//TEST TODO
		DM.tables = "X,Y".split(",");
		TM.codes = new String[]{ "test/AbortTrx.txt", "test/LongGeneralTrx.txt",
				"test/LongReadTrx.txt","test/ShortAggrTrx.txt",
				"test/ShortGeneralTrx.txt","test/ShortReadTrx.txt","test/ShortTrxNoAggr.txt"};
		//Test end ,"test/LongTrxNoAggrTrx.txt"
		
		/*
		 * Getting input from user
		 */
//		 System.out.println("Please input your script(transaction) files, separated by comma (,): ");
//		 String tmp = getInput();
//		 TM.codes = tmp.split(",");
//		 for(int i=0;i<TM.codes.length;i++){
//			 TM.codes[i]="test/"+ TM.codes[i].trim();
//		 }
//		
//		 int flag;
//		 do{
//			 flag =1;
//			 System.out.println("Please input the buffer size (in Pages), the fefault value is 524288 bytes: ");
//			 tmp = getInput();
//		 try{
//			 DM.bufferSize = Integer.parseInt(tmp);
//		 }catch(Exception e){
//			 flag = 0;
//			 System.out.println("Invalid input.");
//		 }
//		 }while(flag==0);
//		
//		 System.out.println("Please input your tables' names, separated by comma (,): ");
//		 tmp = getInput();
//		 DM.tables = tmp.split(",");
//		 for(int i=0;i<DM.tables.length;i++){
//			 DM.tables[i] = DM.tables[i].trim();
//		 }
//		
//		
//		 do{
//		 flag =1;
//		 System.out.println("Please input the seed of the random number generator: ");
//		 tmp = getInput();
//		 try{
//			 TM.randomSeed = Integer.parseInt(tmp);
//		 }catch(Exception e){
//			 flag = 0;
//			 System.out.println("Invalid input.");
//		 }
//		 }while(flag==0);
//		
//		System.out.println("Please select concurrent reading option. Enter 0 for round robin, anything else for random. The default option is round robin");
//		tmp = getInput();
//		if(!tmp.equals("0")){
//			TM.concurrentReadOption = 2;
//		}
//		
		
		 /*
		  * Initialization
		  */
		 System.out.println("[Initialization] starts");
		 
		 for(int i=0;i<DM.tables.length;i++){
			 L1_delta.tablePages.put(DM.tables[i], new ArrayList());
			 L2_delta.tablePages.put(DM.tables[i]+"-ClientName", new ArrayList());
			 L2_delta.tablePages.put(DM.tables[i]+"-Phone", new ArrayList());
			 Disk.tablePages.put(DM.tables[i]+"-ClientName", new ArrayList());
			 Disk.tablePages.put(DM.tables[i]+"-Phone", new ArrayList());
		 }
		 
		 System.out.println("Start to load data from data tables.");
		 ArrayList loadInst = Utils.loadData(DM.tables);
		 DM.execute(loadInst);
		 System.out.println("All the initial data have been loaded to PittHN.");
		
		
		System.out.println("\n[Transaction Manager]");
		TM tranManager=new TM(); 
		TM.execute();
		if(TM.instructionList.size()<1){
			System.out.println("No transactions received. ");
			System.out.println("*** PittHN finishes executing ***");
			System.exit(0);
		}
		
		
		System.out.println("\n[Scheduler]");
		LockTable lockTable=new LockTable(); 
		DM dataManager=new DM();
		Scheduler scheduler=new Scheduler( lockTable,  tranManager,  dataManager);
		scheduler.Schedule_Transactions();
		
		System.out.println("[DataManager] Flush all data in L1-delta and L2-delta to the disk files.");
		DM.Finish();
		
	//	DM.execute(TM.instructionList);
		
		System.out.println("\n[Statistics Analysis]");
		
		System.out.println("Time for each transaction (in nanoseconds): ");
		Iterator it = DM.transactionStartTimeMap.entrySet().iterator();
		while(it.hasNext()){
			Entry e = (Entry) it.next();
			String key = (String) e.getKey();
			if(key.equals("-1--1"))
				continue;
			long value = (long) e.getValue();
			
			System.out.print("Transaction <"+key+">: ");
			if((DM.transactionFinishTimeMap.get(key))!=null){
				System.out.println(DM.transactionFinishTimeMap.get(key)-value);
			}else{
				System.out.println("----");
			}
		}
		
		if(DM.counterRead>0)
			System.out.println("Average time for R (read) operation: "+ DM.overallReadTime/DM.counterRead);
		
		if(DM.counterCommit>0)
			System.out.println("Average time for C (commit) operation: "+DM.overallCommitTime /DM.counterCommit );
		
		if(DM.counterAbort>0)
			System.out.println("Average time for A (abort) operation: "+DM.overallAbortTime /DM.counterAbort );
		
		if(DM.counterMultipleRead>0)
			System.out.println("Average time for M (mutiple read) operation: "+DM.overallMultipleReadTime  /DM.counterMultipleRead );
		
		if(DM.counterWrite>0)
			System.out.println("Average time for W (write) operation: "+DM.overallWriteTime /DM.counterWrite );
		
		if(DM.counterDelete>0)
			System.out.println("Average time for D (delete) operation: "+DM.overallDeleteTime /DM.counterDelete );
		
		if(DM.counterG>0)
			System.out.println("Average time for G (group count) operation: "+DM.overallGTime /DM.counterG  );
		
		System.out.println("\n*** PittHN finishes executing ***");
	}
	
	
	
	
	public static String getInput(){
		String str="";
		 try {
	            InputStreamReader is_reader = new InputStreamReader(System.in);
	            str = new BufferedReader(is_reader).readLine();
	            
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		 return str.trim();
	}

}
