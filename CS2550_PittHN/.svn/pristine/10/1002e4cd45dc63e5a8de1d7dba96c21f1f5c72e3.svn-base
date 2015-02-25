package Scheduler;
import Common.*;
//element for lock table
//<transactionID, lockType>
//LockType: <1, IS> <2, IX> <3, S> <4, X>
public class LockTableElement
{
	public static final int IS=1;
	public static final int IX=2;
	public static final int S=3;
	public static final int X=4;
	
	private String transactionID;
	private int lockType;
	
	//TODO 
	/**
	 *   
	 * @param cmd
	 * @return
	 */
	public static LockTableElement CONSTRUCT_LTE(Instruction cmd){
		int lockType=0;
		if(cmd.isReadCommand()){
			lockType=3;
		}else if(cmd.isWriteCommand()){
			lockType=4;
		}else if(cmd.isDeleteCommand()){
			lockType=4;
		}else if(cmd.isMatchCommand()){
			lockType=3;
		}else if(cmd.isGCommand()){
			lockType=3;
		}
		
		return new LockTableElement(cmd.transactionID,lockType);
	}
	
	
	
	public LockTableElement(String transactionID, int lockType)
	{
		this.transactionID = transactionID;
		this.lockType = lockType;
	}
	
	public String getTransactionID()
	{
		return transactionID;
	}
	
	public int getLockType()
	{
		return lockType;
	}
	
	public void setLockType(int lockType)
	{
		this.lockType = lockType;
	}
	
	public boolean equals(LockTableElement e)
	{
		if(this.transactionID.equals(e.getTransactionID()) && this.lockType == e.getLockType())
			return true;
		else
			return false;
	}
	
	public String toString()
	{
		String s = "<" + transactionID + ", " + lockType + ">";
		return s;
	}
	/*
	public static void main(String[] args)
	{
		LockTableElement lte = new LockTableElement("T1", 2);
		System.out.println(lte.equals(new LockTableElement("T1", 2)));
		System.out.println(lte.equals(new LockTableElement("T1", 1)));
	}*/
}
