package Scheduler;
import java.util.*;
import Common.*;

public class LockTable
{
	private HashMap<String, LockTableValue> tabLock;
	private HashMap<String, LockTableValue> recLock;
	private HashMap<String, Integer> retrieveLockType;
	private HashMap<String, String> retrieveRecID;
	private ArrayList<String> txInLock;
	//private HashMap<String, String> retrieveTabID;
	
	public LockTable()
	{
		tabLock = new HashMap<String, LockTableValue>();
		recLock = new HashMap<String, LockTableValue>();
		retrieveLockType = new HashMap<String, Integer>();
		retrieveRecID = new HashMap<String, String>();
		txInLock = new ArrayList<String>();
	}
	//request a table level lock
	public boolean add(LockTableElement e, String tableID)
	{
		LockTableValue ltv;
		boolean re;
		
		if(!txInLock.contains(e.getTransactionID()))
		{
			txInLock.add(e.getTransactionID());
		}
		//retrieveLockType.put(e.getTransactionID(), e.getLockType());
		//retrieveTabID.put(e.getTransactionID(), tableID);
		if(e.getLockType() < 1 || e.getLockType() > 4)
		{
			System.out.println("add(LockTableElement e, String tableID): lock type error!");
			System.exit(1);
			return false;
		}
		if(tabLock.containsKey(tableID))
			ltv = tabLock.get(tableID);
		else
			ltv = new LockTableValue();
		re = ltv.add(e);
		tabLock.put(tableID, ltv);
		return re;
	}
	//request a record level lock
	public boolean add(LockTableElement e, String tableID, String recordID)
	{
		LockTableValue ltv;
		boolean re;
		//change S(X) on record to IS(IX) on table
		LockTableElement temp = new LockTableElement(e.getTransactionID(), e.getLockType() - 2);
		
		if(!txInLock.contains(e.getTransactionID()))
		{
			txInLock.add(e.getTransactionID());
		}
		//retrieveLockType.put(e.getTransactionID(), e.getLockType());
		//retrieveRecID.put(e.getTransactionID(), recordID);
		//retrieveTabID.put(e.getTransactionID(), tableID);
		if(e.getLockType() < 3 || e.getLockType() > 4)
		{
			System.out.println("add(LockTableElement e, String tableID, String recordID): lock type error!");
			System.exit(1);
			return false;
		}
		//add table level lock first
		if(tabLock.containsKey(tableID))
			ltv = tabLock.get(tableID);
		else
			ltv = new LockTableValue();
		re = ltv.add(temp);
		tabLock.put(tableID, ltv);
		if(!re) //fail to get table level lock
		{
			retrieveLockType.put(e.getTransactionID(), e.getLockType());
			retrieveRecID.put(e.getTransactionID(), recordID);
			return false;
		}
		//then add record level lock
		else
		{
			recordID = tableID + "_" + recordID;
			if(recLock.containsKey(recordID))
				ltv = recLock.get(recordID);
			else
				ltv = new LockTableValue();
			re = ltv.add(e);
			recLock.put(recordID, ltv);
			return re;
		}
	}
	
	//remove all the locks of a transaction when it finishes
	//return (at most one) transactionID if it is unblocked from waiting list 
	//or return "" when no transaction is unblocked
	public String remove(String transactionID)
	{
		String releasedTxID  = "";
		
		//remove from record level lock table
		Iterator<String> lockTableIterator = recLock.keySet().iterator();
		while(lockTableIterator.hasNext())
		{
			String key = lockTableIterator.next();
			LockTableValue value = recLock.get(key);
			String s = value.remove(transactionID);
			if(s.length() > 0)
			{
				releasedTxID = s;
			}
		}
		
		//remove from table level lock table
		lockTableIterator = tabLock.keySet().iterator();
		while(lockTableIterator.hasNext())
		{
			String key = lockTableIterator.next();
			LockTableValue value = tabLock.get(key);
			String s = value.remove(transactionID);
			//if unlock a transaction
			if(s.length() > 0)
			{
				//if a record level lock is blocked at the table level
				if(retrieveLockType.containsKey(s))
				{
					String dataID = retrieveRecID.get(s);
					int lType = retrieveLockType.get(s);
					
					if(add(new LockTableElement(s, lType), key, dataID))
						releasedTxID = s;
				}
				
			}
		}
		txInLock.remove(transactionID);
		
		return releasedTxID;
		
	}
	
	
	//return a victim if there is a deadlock, or "" if not
	public String detectDeadlock()
	{
		int nodeCount = txInLock.size();
		int[][] adjMat = new int[nodeCount][nodeCount];
		Iterator<String> lockTableIterator;
		Stack<Integer> stack = new Stack<Integer>();
		Stack<Integer> path = new Stack<Integer>();
		
		//construct adjacent matrix
		for(int i = 0; i < nodeCount; i++)
			for(int j = 0; j < nodeCount; j++)
				adjMat[i][j] = 0;
		//record level lock table
		lockTableIterator = recLock.keySet().iterator();
		while(lockTableIterator.hasNext())
		{
			String key = lockTableIterator.next();
			LockTableValue value = recLock.get(key);
			int currentSize = value.getCurrentList().size();
			int waitingSize = value.getWaitingList().size();
			
			for(int i = 0; i < currentSize; i++)
			{
				String t1 = value.getCurrentList().get(i).getTransactionID();
				for(int j = 0; j < waitingSize; j++)
				{
					String t2 = value.getWaitingList().get(j).getTransactionID();
					adjMat[txInLock.indexOf(t1)][txInLock.indexOf(t2)] = 1;
				}
			}
		}
		//table level lock table
		lockTableIterator = tabLock.keySet().iterator();
		while(lockTableIterator.hasNext())
		{
			String key = lockTableIterator.next();
			LockTableValue value = tabLock.get(key);
			int currentSize = value.getCurrentList().size();
			int waitingSize = value.getWaitingList().size();
			
			for(int i = 0; i < currentSize; i++)
			{
				String t1 = value.getCurrentList().get(i).getTransactionID();
				for(int j = 0; j < waitingSize; j++)
				{
					String t2 = value.getWaitingList().get(j).getTransactionID();
					int index1 = txInLock.indexOf(t1);
					int index2 = txInLock.indexOf(t2);
					
					System.out.println("index1 = " + index1 + ", index2 = " + index2);
					adjMat[txInLock.indexOf(t1)][txInLock.indexOf(t2)] = 1;
				}
			}
		}
		//output for debug
		for(int i = 0; i < nodeCount; i++)
		{
			for(int j = 0; j < nodeCount; j++)
			{
				System.out.print(adjMat[i][j] + "\t");
			}
			System.out.println();
		}
		//detect deadlock with adjacent matrix
		/*
		nodeCount = 8;
		adjMat = new int[nodeCount][nodeCount];
		adjMat[0][1] = 1;
		adjMat[0][2] = 1;
		adjMat[1][3] = 1;
		adjMat[2][4] = 1;
		adjMat[2][5] = 1;
		adjMat[5][6] = 1;
		adjMat[5][7] = 1;*/
		for(int i = 0; i < nodeCount; i++)
		{
			stack.clear();
			path.clear();
			stack.push(i);
			System.out.println();
			while(!stack.empty())
			{
				//System.out.println("tree: " + stack);
				int node = stack.pop();
				boolean flag = false;
				
				if(path.contains(node))
				{
					return txInLock.get(node);
				}
				else
					path.push(node);
				//System.out.println("path: " + path);
				for(int j = 0; j < nodeCount; j++)
				{
					if(adjMat[node][j] == 1)
					{
						stack.push(j);
						flag = true;
					}
				}
				if(!flag)
				{
					if(stack.empty())
						continue;
					path.pop();
					while(adjMat[path.peek()][stack.peek()] != 1)
					{
						path.pop();
					}
				}
			}
		}
		return "";
		
	}
	
	public TwoArrayList resolveDeadlock()
	{
		ArrayList<String> killed = new ArrayList<String>();
		ArrayList<String> unblocked = new ArrayList<String>();
		TwoArrayList re;
		String temp1;
		String temp2;
		
		temp1 = detectDeadlock();
		while(temp1.length() != 0)
		{
			killed.add(temp1);
			System.out.println("Killing " + temp1);
			System.out.println("table lock" + tabLock);
			System.out.println("record lock" + recLock);
			temp2 = remove(temp1);
			
			System.out.println("table lock" + tabLock);
			System.out.println("record lock" + recLock);
			if(temp2.length() != 0)
			{
				unblocked.add(temp2);
			}
			temp1 = detectDeadlock();		
		}
		re = new TwoArrayList(killed, unblocked);
		System.out.println("Killed: " + killed);
		System.out.println("Unblocked: " + unblocked);
		return re;
		
	}
	
	
	public String toString()
	{
		String s = "\nTable level\n" + tabLock + "\nRecord level\n" + recLock + "\n";
		return s;
	}
	/*
	public static void main(String[] args)
	{
		LockTable lt = new LockTable();
		lt.add(new LockTableElement("T1", 4), "t1", "r1");
		System.out.println(lt.detectDeadlock());
		lt.add(new LockTableElement("T2", 4), "t1", "r2");
		System.out.println(lt.detectDeadlock());
		lt.add(new LockTableElement("T3", 4), "t1", "r3");
		System.out.println(lt.detectDeadlock());
		lt.add(new LockTableElement("T1", 4), "t1", "r3");
		System.out.println(lt.detectDeadlock());
		lt.add(new LockTableElement("T2", 4), "t1", "r1");
		System.out.println(lt.detectDeadlock());
		lt.add(new LockTableElement("T3", 4), "t1", "r2");
		System.out.println(lt.detectDeadlock());
		System.out.println(lt);
		lt.resolveDeadlock();
	
	}*/
	//TODO
	
}
