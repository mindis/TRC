package DataManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;

import Common.Instruction;

public class DM {
	public static int bufferSize = 1000000;//524288;  //in Pages
	
	public static int PAGE_SIZE = 512;  //in byte
	
	public static int L1_PAGE_SIZE = PAGE_SIZE/34; // (15) number of records in L1 page 
	
	public static int L2_PAGE_SIZE1 = PAGE_SIZE/22; // (23) number of records for column 1 in L2/Disk page 
	
	public static int L2_PAGE_SIZE2 = PAGE_SIZE/16; // (32) number of records for column 2 in L2/Disk page 
	
	public static int L1_SIZE = bufferSize/4; //in pages
	
	public static int L2_SIZE = bufferSize/4*3; //in pages
	
	public static ArrayList<DMLog> DMLogger = new ArrayList<DMLog>();
	
	public static HashMap<String, Stack> transactionLogMap = new HashMap<String, Stack>();
	
	public static String[] tables = null; 
	
	/*
	 * Used for "D table" operation
	 * Before commit, the related pages will be marked as "deleted" while keeping the before image,
	 * the pages will be cleared when committing, or revert to original otherwise
	 */
	public static HashMap<String, ArrayList> toDeleteMap = new HashMap<String, ArrayList>();
	
	/*
	 * Statistics
	 */
	public static HashMap<String, Long> transactionStartTimeMap = new HashMap<String, Long>();
	public static HashMap<String, Long> transactionFinishTimeMap = new HashMap<String, Long>();
	public static long overallReadTime = 0;
	public static long counterRead = 0;
	public static long overallCommitTime = 0;
	public static long counterCommit = 0;
	public static long overallAbortTime = 0;
	public static long counterAbort = 0;
	public static long overallMultipleReadTime = 0;
	public static long counterMultipleRead = 0;
	public static long overallWriteTime = 0;
	public static long counterWrite = 0;
	public static long overallDeleteTime = 0;
	public static long counterDelete = 0;
	public static long overallGTime = 0;
	public static long counterG = 0;
	
	
	public static void execute(ArrayList<Instruction> instructionList){
		Iterator it = instructionList.iterator();
		while(it.hasNext()){
			Instruction inst = (Instruction) it.next();
			System.out.println("[DataManager] Executing operation: <"+inst.transactionID+"> "+inst.inst);
			
			if(transactionStartTimeMap.get(inst.transactionID)==null){
				transactionStartTimeMap.put(inst.transactionID, Utils.getCurrentTime());
			}
			
			if(inst.command.equalsIgnoreCase("B")){
				DMLog log = new DMLog(inst.transactionID, inst.command, null, null);
				transactionLogMap.put(inst.transactionID, new Stack<DMLog>());
				DMLogger.add(log);
				System.out.println("[DataManager] Transaction <"+inst.transactionID+"> Starts.");
				
				transactionStartTimeMap.put(inst.transactionID, Utils.getCurrentTime());
				
			}else if(inst.command.equalsIgnoreCase("C")){
				DMLog log = new DMLog(inst.transactionID, inst.command, null, null);
				Commit(inst.transactionID);
				log.setFinishtTimestamp();
				DMLogger.add(log);
				DM.transactionLogMap.remove(inst.transactionID);
				System.out.println("[DataManager] Transaction <"+inst.transactionID+"> has been committed.");
				
				transactionFinishTimeMap.put(inst.transactionID, Utils.getCurrentTime());
				counterCommit++;
				overallCommitTime = overallCommitTime + log.timeOfRequestFinished -log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("A") || inst.command.equalsIgnoreCase("K")){
				DMLog log = new DMLog(inst.transactionID, inst.command, null, null);
				Abort(inst.transactionID);
				log.setFinishtTimestamp();
				DMLogger.add(log);
				DM.transactionLogMap.remove(inst.transactionID);
				System.out.println("[DataManager] Transaction <"+inst.transactionID+"> has been aborted.");
				
				transactionFinishTimeMap.put(inst.transactionID, Utils.getCurrentTime());
				counterAbort ++;
				overallAbortTime  = overallAbortTime  + log.timeOfRequestFinished -log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("R")){
				DMLog log = new DMLog(inst.transactionID, inst.command, inst.table, inst.data);
				Tuple result = Read(inst);
				log.setFinishtTimestamp();
				DMLogger.add(log);
				System.out.print("[DataManager] Read result: ");
				if(result == null){
					System.out.println("No record found.");
				}else{
					System.out.println("\n\tID\tClientName\tPhone");
					System.out.println("\t"+result.ID+"\t"+result.ClientName+"\t"+result.Phone);
				}
				
				
				counterRead  ++;
				overallReadTime   = overallReadTime  + log.timeOfRequestFinished -log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("M")){
				DMLog log = new DMLog(inst.transactionID, inst.command, inst.table, inst.data);
				ArrayList result = MultipleRead(inst.table,inst.data);
				log.setFinishtTimestamp();
				DMLogger.add(log);
				System.out.println("[DataManager] Multiple read result: "+ result.size()+" records found.");
				if(result.size()>0)
					System.out.println("\tID\tClientName\tPhone");
				for(int i=0;i<result.size();i++){
					Tuple tmp = (Tuple) result.get(i);
					System.out.println("\t"+tmp.ID+"\t"+tmp.ClientName+"\t"+tmp.Phone);
				}
				
				counterMultipleRead   ++;
				overallMultipleReadTime    = overallMultipleReadTime   + log.timeOfRequestFinished -log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("W")){
				DMLog log = new DMLog(inst.transactionID, inst.command, inst.table, inst.data);
				Write(inst);
				log.setFinishtTimestamp();
				if(transactionLogMap.get(inst.transactionID)==null){
					transactionLogMap.put(inst.transactionID, new Stack<DMLog>());
				}
				transactionLogMap.get(inst.transactionID).push(log);
				DMLogger.add(log);
				System.out.println("[DataManager] 1 record has been added. ");
				
				counterWrite    ++;
				overallWriteTime     = overallWriteTime    + log.timeOfRequestFinished -log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("D")){
				DMLog log = new DMLog(inst.transactionID, inst.command, inst.table, "");
				Delete(inst.table, log.LogID);
				log.setFinishtTimestamp();
				if(transactionLogMap.get(inst.transactionID)==null){
					transactionLogMap.put(inst.transactionID, new Stack<DMLog>());
				}
				transactionLogMap.get(inst.transactionID).push(log);
				DMLogger.add(log);
				
				/*
				 * For commit all the deletions
				 */
				if(DM.toDeleteMap.get(inst.transactionID)==null){
					DM.toDeleteMap.put(inst.transactionID, new ArrayList());
				}
				DM.toDeleteMap.get(inst.transactionID).add(inst.table);
				
				counterDelete ++;
				overallDeleteTime      = overallDeleteTime     + log.timeOfRequestFinished-log.timeOfRequestReceived;
				
			}else if(inst.command.equalsIgnoreCase("G")){
				DMLog log = new DMLog(inst.transactionID, inst.command, inst.table, inst.data);
				int result = Count(inst.table,inst.data);
				log.setFinishtTimestamp();
				DMLogger.add(log);
				System.out.println("[Operation G] Result: "+result);
				
				counterG   ++;
				overallGTime = overallGTime  + log.timeOfRequestFinished-log.timeOfRequestReceived ;
				
			}else if(inst.command.equalsIgnoreCase("E")){
				Finish();
			}
		}
	}

	
	/*
	 * Command 'R'
	 */
	public static Tuple Read(Instruction inst){
		
		 // Read from L1-delta
		ArrayList pages = L1_delta.tablePages.get(inst.table);
		int[] target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			Page page = L1_delta.buffer[ target[i]];
			if(page.checkIfDeleted()){
				continue;
			}else{
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple row = (Tuple) it.next();
					if(inst.data.equals(String.valueOf(row.ID))){
						return row;
					}
				}
			}
		}
		
		//Read from L2-delta
		boolean foundCol1 = false;
		Tuple result = null;
		pages = L2_delta.tablePages.get(inst.table+"-"+"ClientName");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length && (!foundCol1);i++){
			Page page = L2_delta.buffer[target[i]];
			if(page.checkIfDeleted()){
				continue;
			}else{
				Iterator it = page.records.iterator();
				while(it.hasNext() && (!foundCol1)){
					Tuple row = (Tuple) it.next();
					if(inst.data.equals(String.valueOf(row.ID))){
						result = new Tuple(row.ID,row.ClientName,null);
						foundCol1 = true;
						page.setTimestamp();
					}
				}
			}
		}
		
		if(!foundCol1){
			pages = Disk.tablePages.get(inst.table+"-"+"ClientName");
			target = new int[pages.size()];
			for(int i=0;i<pages.size();i++){
				target[i]=(int) pages.get(i);
			}
			for(int i=0;i<target.length && (!foundCol1);i++){
				int l2 = Utils.loadDiskPageToL2(target[i]);
				Page page = L2_delta.buffer[l2];
				if(page.checkIfDeleted()){
					continue;
				}else{
					Iterator it = page.records.iterator();
					while(it.hasNext() && (!foundCol1)){
						Tuple row = (Tuple) it.next();
						if(inst.data.equals(String.valueOf(row.ID))){
							if(result==null){
								result = new Tuple(row.ID,row.ClientName,null);
							}else{
								result.Phone = row.Phone;
							}
							foundCol1 = true;
							page.setTimestamp();
						}
					}
				}
			}
		}
		
		if(!foundCol1)
			return null;
		
		boolean foundCol2 = false;
		pages = L2_delta.tablePages.get(inst.table+"-"+"Phone");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length && (!foundCol2);i++){
			Page page = L2_delta.buffer[target[i]];
			if(page.checkIfDeleted()){
				continue;
			}else{
				Iterator it = page.records.iterator();
				while(it.hasNext() && (!foundCol2)){
					Tuple row = (Tuple) it.next();
					if(inst.data.equals(String.valueOf(row.ID))){
						result.Phone = row.Phone;
						foundCol2 = true;
						page.setTimestamp();
					}
				}
			}
		}
		
		if(!foundCol2){
			pages = Disk.tablePages.get(inst.table+"-"+"Phone");
			target = new int[pages.size()];
			for(int i=0;i<pages.size();i++){
				target[i]=(int) pages.get(i);
			}
			for(int i=0;i<target.length && (!foundCol2);i++){
				int l2 = Utils.loadDiskPageToL2(target[i]);
				Page page = L2_delta.buffer[l2];
				if(page.checkIfDeleted()){
					continue;
				}else{
					Iterator it = page.records.iterator();
					while(it.hasNext() && (!foundCol2)){
						Tuple row = (Tuple) it.next();
						if(inst.data.equals(String.valueOf(row.ID))){
							result.Phone = row.Phone;
							foundCol2 = true;
							page.setTimestamp();
						}
					}
				}
			}
		}
		 
		return result;
		
	}
	
	/*
	 * Command 'M table val'
	 */
	public static ArrayList MultipleRead(String table, String areaCode){
		ArrayList result = new ArrayList();
		
		ArrayList pages = L1_delta.tablePages.get(table);
		int[] target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			Page page = L1_delta.buffer[target[i]];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						result.add(tuple);
						page.setTimestamp();
					}
						
				}
			}
		}
		
		HashMap map = new HashMap();
		pages = L2_delta.tablePages.get(table+"-Phone");
		target = new int[pages.size()];
		for(int i=0;i<target.length;i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			Page page = L2_delta.buffer[target[i]];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						map.put(tuple.ID, new Tuple(tuple.ID,null,tuple.Phone));
						page.setTimestamp();
					}
						
				}
			}
		}
		
		pages = Disk.tablePages.get(table+"-Phone");
		target = new int[pages.size()];
		for(int i=0;i<target.length;i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			int l2Index = Utils.loadDiskPageToL2(target[i]);
			Page page = L2_delta.buffer[l2Index];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						map.put(tuple.ID, new Tuple(tuple.ID,null,tuple.Phone));
						page.setTimestamp();
					}
						
				}
			}
		}
		
		pages = L2_delta.tablePages.get(table+"-ClientName");
		target = new int[pages.size()];
		for(int i=0;i<target.length;i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			Page page = L2_delta.buffer[target[i]];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					Tuple rTuple = (Tuple) map.get(tuple.ID);
					if(rTuple!=null){
						rTuple.ClientName = tuple.ClientName;
						page.setTimestamp();
					}
						
				}
			}
		}
		
		pages = Disk.tablePages.get(table+"-ClientName");
		target = new int[pages.size()];
		for(int i=0;i<target.length;i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			int l2Index = Utils.loadDiskPageToL2(target[i]);
			Page page = L2_delta.buffer[l2Index];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					Tuple rTuple = (Tuple) map.get(tuple.ID);
					if(rTuple!=null){
						rTuple.ClientName = tuple.ClientName;
						page.setTimestamp();
					}
				}
			}
		}
		
		Iterator it = map.entrySet().iterator();
		while(it.hasNext()){
			Entry e = (Entry) it.next();
			result.add(e.getValue());
			
		}
		
		return result;
	}
	
	/*
	 * Command 'A'
	 */
	public static void Abort(String transactionID){
		Stack logs = transactionLogMap.get(transactionID);
		while(!logs.isEmpty()){
			DMLog log = (DMLog) logs.pop();
			if(log.operation.equalsIgnoreCase("D")){
				ArrayList pages = L1_delta.tablePages.get(log.table);
				int[] target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					Page page = L1_delta.buffer[target[i]];
					if(page!=null && page.checkIfDeleted() && page.getDeleteLogID()==log.LogID){
						page.undoDelete();
					}
				}
				
				pages = L2_delta.tablePages.get(log.table+"-ClientName");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					Page page = L1_delta.buffer[target[i]];
					if(page!=null && page.checkIfDeleted() && page.getDeleteLogID()==log.LogID){
						page.undoDelete();
					}
				}
				
				pages = Disk.tablePages.get(log.table+"-ClientName");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					Page page = L1_delta.buffer[target[i]];
					if(page!=null && page.checkIfDeleted() && page.getDeleteLogID()==log.LogID){
						page.undoDelete();
					}
				}
				
				pages = L2_delta.tablePages.get(log.table+"-Phone");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					Page page = L1_delta.buffer[target[i]];
					if(page!=null && page.checkIfDeleted() && page.getDeleteLogID()==log.LogID){
						page.undoDelete();
					}
				}
				
				pages = Disk.tablePages.get(log.table+"-Phone");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					Page page = L1_delta.buffer[target[i]];
					if(page!=null && page.checkIfDeleted() && page.getDeleteLogID()==log.LogID){
						page.undoDelete();
					}
				}
				
			}else if(log.operation.equalsIgnoreCase("W")){
				String ID = log.data.split(",")[0].trim();
				
				// delete from L1-delta
				ArrayList pages = L1_delta.tablePages.get(log.table);
				int[] target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int index = target[i];
					Page page = L1_delta.buffer[index];
					if(page.checkIfDeleted()){
						continue;
					}else{
						for(int j=0;j<page.records.size();j++){
							Tuple row = (Tuple) page.records.get(j);
							if(ID.equals(String.valueOf(row.ID))){
								page.records.remove(j);
								if(page.records.size()<1){
									pages.remove(i);
									L1_delta.pageAvailability[index]=0;
								}
								if(L1_delta.pageAvailability[index]==2){
									L1_delta.pageAvailability[index]=1;
								}
								return;
							}
						}
					}
				}
				
				//Delete from L2-delta
				boolean foundCol1 = false;
				pages = L2_delta.tablePages.get(log.table+"-ClientName");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int index = target[i];
					Page page = L2_delta.buffer[index];
					if(page.checkIfDeleted()){
						continue;
					}else{
						for(int j=0;j<page.records.size();j++){
							Tuple row = (Tuple) page.records.get(j);
							if(ID.equals(String.valueOf(row.ID))){
								page.records.remove(j);
								if(page.records.size()<1){
									pages.remove(i);
									L2_delta.pageAvailability[index]=0;
								}
								if(L2_delta.pageAvailability[index]==2){
									L2_delta.pageAvailability[index]=1;
								}
								foundCol1 = true;
							}
						}
					}
				}
				
				if(!foundCol1){
					pages = Disk.tablePages.get(log.table+"-"+"ClientName");
					target = new int[pages.size()];
					for(int i=0;i<pages.size();i++){
						target[i]=target[i];
					}
					for(int i=0;i<target.length && (!foundCol1);i++){
						int l2 = Utils.loadDiskPageToL2(target[i]);
						Page page = L2_delta.buffer[l2];
						if(page.checkIfDeleted()){
							continue;
						}else{
							for(int j=0;j<page.records.size();j++){
								Tuple row = (Tuple) page.records.get(j);
								if(ID.equals(String.valueOf(row.ID))){
									page.records.remove(j);
									if(page.records.size()<1){
										pages.remove(i);
										L2_delta.pageAvailability[l2]=0;
									}
									if(L2_delta.pageAvailability[l2]==2){
										L2_delta.pageAvailability[l2]=1;
									}
									foundCol1 = true;
								}
							}
						}
					}
				}
				
				
				boolean foundCol2 = false;
				pages = L2_delta.tablePages.get(log.table+"-Phone");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=target[i];
				}
				for(int i=0;i<target.length;i++){
					int index = (int)pages.get(i);
					Page page = L2_delta.buffer[index];
					if(page.checkIfDeleted()){
						continue;
					}else{
						for(int j=0;j<page.records.size();j++){
							Tuple row = (Tuple) page.records.get(j);
							if(ID.equals(String.valueOf(row.ID))){
								page.records.remove(j);
								if(page.records.size()<1){
									pages.remove(i);
									L2_delta.pageAvailability[index]=0;
								}
								if(L2_delta.pageAvailability[index]==2){
									L2_delta.pageAvailability[index]=1;
								}
								foundCol2 = true;
							}
						}
					}
				}
				
				if(!foundCol2){
					pages = Disk.tablePages.get(log.table+"-"+"Phone");
					target = new int[pages.size()];
					for(int i=0;i<pages.size();i++){
						target[i]=(int) pages.get(i);
					}
					for(int i=0;i<target.length && (!foundCol2);i++){
						int l2 = Utils.loadDiskPageToL2(target[i]);
						Page page = L2_delta.buffer[l2];
						if(page.checkIfDeleted()){
							continue;
						}else{
							for(int j=0;j<page.records.size();j++){
								Tuple row = (Tuple) page.records.get(j);
								if(ID.equals(String.valueOf(row.ID))){
									page.records.remove(j);
									if(page.records.size()<1){
										pages.remove(i);
										L2_delta.pageAvailability[l2]=0;
									}
									if(L2_delta.pageAvailability[l2]==2){
										L2_delta.pageAvailability[l2]=1;
									}
									foundCol2 = true;
								}
							}
						}
					}
				}
				 
			}
		}
	}
	
	/*
	 * 1. Delete all pages which have been set to delete by the specified transaction
	 * 2. Remove logs of this transaction
	 */
	public static void Commit(String transactionID){
		ArrayList deletedTables = toDeleteMap.get(transactionID);
		if(deletedTables!=null && deletedTables.size()>0){
			Iterator it = deletedTables.iterator();
			while(it.hasNext()){
				String table = (String) it.next();
				
				ArrayList pages = L1_delta.tablePages.get(table);
				int[] target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int index = target[i];
					if(L1_delta.buffer[index].checkIfDeleted()){
						L1_delta.pageAvailability[index] = 0;
						L1_delta.tablePages.get(table).remove(new Integer(index));
						L1_delta.buffer[index]=null;
					}
				}
				
				pages = L2_delta.tablePages.get(table+"-ClientName");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int index =target[i];
					if(L2_delta.buffer[index].checkIfDeleted()){
						L2_delta.pageAvailability[index] = 0;
						L2_delta.tablePages.get(table+"-ClientName").remove(new Integer(index));
						L2_delta.buffer[index]=null;
						
					}
				}
				
				pages = Disk.tablePages.get(table+"-ClientName");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int l2 = Utils.loadDiskPageToL2(target[i]);
					if( L2_delta.buffer[l2].checkIfDeleted()){
						L2_delta.buffer[l2] = null;
						L2_delta.tablePages.get(table+"-ClientName").remove(new Integer(l2));
						L2_delta.pageAvailability[l2] = 0;
					}
				}
				
				pages = L2_delta.tablePages.get(table+"-Phone");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int index = target[i];
					if(L2_delta.buffer[index].checkIfDeleted()){
						L2_delta.pageAvailability[index] = 0;
						L2_delta.tablePages.get(table+"-Phone").remove(new Integer(index));
						L2_delta.buffer[index]=null;
					}
				}
				
				pages = Disk.tablePages.get(table+"-Phone");
				target = new int[pages.size()];
				for(int i=0;i<pages.size();i++){
					target[i]=(int) pages.get(i);
				}
				for(int i=0;i<target.length;i++){
					int l2 = Utils.loadDiskPageToL2(target[i]);
					if( L2_delta.buffer[l2].checkIfDeleted()){
						L2_delta.buffer[l2] = null;
						L2_delta.tablePages.get(table+"-Phone").remove(new Integer(l2));
						L2_delta.pageAvailability[l2] = 0;
					}
				}
				
			}
		}
		
	}
	
	public static void Write(Instruction inst){
		int index = Utils.findAvailablePageInL1(inst.table);
		String[] data = inst.data.split(",");
		Tuple row = new Tuple(Integer.valueOf(data[0].trim()),data[1].trim(),data[2].trim());
		L1_delta.buffer[index].records.add(row);
		if(L1_delta.buffer[index].records.size()==DM.L1_PAGE_SIZE){
			L1_delta.pageAvailability[index]=2;
		}
		L1_delta.buffer[index].setTimestamp();
	}
	
	
	public static void Delete(String table, int logID){
		ArrayList pages = L1_delta.tablePages.get(table);
		int[] target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			if(!L1_delta.buffer[target[i]].checkIfDeleted()){
				L1_delta.buffer[target[i]].setToBeDeleted(logID);
			}
		}
		
		pages = L2_delta.tablePages.get(table+"-ClientName");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			if(!L2_delta.buffer[target[i]].checkIfDeleted()){
				L2_delta.buffer[target[i]].setToBeDeleted(logID);
			}
		}
		
		pages = Disk.tablePages.get(table+"-ClientName");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			int l2Index = Utils.loadDiskPageToL2(target[i]);
			if(!L2_delta.buffer[l2Index].checkIfDeleted()){
				L2_delta.buffer[l2Index].setToBeDeleted(logID);
			}
		}
		
		pages = L2_delta.tablePages.get(table+"-Phone");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			if(!L2_delta.buffer[target[i]].checkIfDeleted()){
				L2_delta.buffer[target[i]].setToBeDeleted(logID);
			}
		}
		
		pages = Disk.tablePages.get(table+"-Phone");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			int l2Index = Utils.loadDiskPageToL2(target[i]);
			if(!L2_delta.buffer[l2Index].checkIfDeleted()){
				L2_delta.buffer[l2Index].setToBeDeleted(logID);
			}
		}
		
		
		
		
	}
	
	public static int Count(String table, String areaCode){
		int result = 0;
		ArrayList pages = L1_delta.tablePages.get(table);
		for(int i=0;i<pages.size();i++){
			Page page = L1_delta.buffer[(int)pages.get(i)];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						result++;
						page.setTimestamp();
					}
						
				}
			}
		}
		
		pages = L2_delta.tablePages.get(table+"-Phone");
		int[] target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			Page page = L2_delta.buffer[target[i]];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						result++;
						page.setTimestamp();
					}
						
				}
			}
		}
		
		pages = Disk.tablePages.get(table+"-Phone");
		target = new int[pages.size()];
		for(int i=0;i<pages.size();i++){
			target[i]=(int) pages.get(i);
		}
		for(int i=0;i<target.length;i++){
			int l2Index = Utils.loadDiskPageToL2(target[i]);
			Page page = L2_delta.buffer[l2Index];
			if(!page.checkIfDeleted()){
				Iterator it = page.records.iterator();
				while(it.hasNext()){
					Tuple tuple = (Tuple) it.next();
					if(tuple.Phone.startsWith(areaCode)){
						result++;
						page.setTimestamp();
					}
						
				}
			}
		}
		
		return result;
	}
	
	public static void Finish(){
		Utils.L1_L2_Merge();
		Utils.flushAllL2ToDisk();
		Utils.WriteDiskFile();
	}
	
	

}
