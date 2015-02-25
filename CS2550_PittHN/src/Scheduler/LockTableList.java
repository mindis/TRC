package Scheduler;

import java.util.*;
//abstraction for current list or waiting list
public class LockTableList
{
	private LinkedList<LockTableElement> list;
	
	public LockTableList()
	{
		this.list = new LinkedList<LockTableElement>();
	}
	//add an element to end of list
	public void add(LockTableElement e)
	{
		this.list.add(e);
	}
	//take the first element from list
	public LockTableElement remove()
	{
		return this.list.remove();
	}
	
	//remove a lock table element for a certain tx
	public boolean remove(String transactinoID)
	{
		boolean re = false;
		LinkedList<LockTableElement> temp = new LinkedList<LockTableElement>();
		LockTableElement e;
		int length = list.size();
		
		for(int i = 0; i < length; i++)
		{
			e = list.remove();
			if(e.getTransactionID().equals(transactinoID))
			{
				re = true;
			}
			else
			{
				temp.add(e);
			}
		}
		list = temp;
		return re;
	}
	//retrieve without removing the first element 
	public LockTableElement retrieve()
	{
		return list.element();
	}
	//check if list contains element
	public boolean contains(LockTableElement e)
	{
		for(int i = 0; i < list.size(); i++)
		{
			if(list.get(i).equals(e))
				return true;
		}
		return false;
	}
	//check if list contains <transaction, *>
	public boolean contains(String transactionID)
	{
		for(int i = 0; i < list.size(); i++)
		{
			if(list.get(i).getTransactionID().equals(transactionID))
				return true;
		}
		return false;
	}
	//check if list is empty
	public boolean isEmpty()
	{
		return list.isEmpty();
	}
	//return list length
	public int size()
	{
		return list.size();
	}
	
	public LockTableElement get(int index)
	{
		return list.get(index);
	}
	
	public String toString()
	{
		int i;
		String s = "";
		if(list.size() == 0)
			return s;
		for(i = 0; i < list.size() - 1; i++)
			s += list.get(i) + ", ";
		s += list.getLast();
		return s;
	}
	/*
	public static void main(String[] args)
	{
		LockTableList ltw = new LockTableList();
		LockTableElement lte = new LockTableElement("T1", 3);
		ltw.add(lte);
		ltw.add(new LockTableElement("T2", 1));
		ltw.add(new LockTableElement("T3", 1));
		System.out.println(ltw);
		ltw.remove("T2");
		System.out.println(ltw);
		ltw.remove("T1");
		System.out.println(ltw);
		
	}*/
	
	public void push(LockTableElement e){
		list.push(e);
	}
}
