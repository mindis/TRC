package DataManager;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

/*
 * Assumption: 
 * 1. Size of the record.
 * int: 4 byte, char: 1 byte
 * So each tuple/record has 4+18+12=24 byte.
 * 
 * 2. Page size is 512 byte. User need to specify the buffer size in number of pages.
 * 
 */

public class Page {
	
	public String tableName = null;
	
	public String colName = null;
	
	public String storeKey = null;

	public ArrayList records = null;
	
	public long ts = 0;
	
	private boolean isDeleted= false;
	
	private int deleteID = -1;
	
	public Page(String storeKey, int type){
		if(type==0){
			this.tableName = storeKey;
			this.storeKey = storeKey;
			this.records = new ArrayList();
		}else{
			this.storeKey = storeKey;
			String[] tmp = storeKey.split("-");
			this.tableName = tmp[0];
			this.colName = tmp[1];
			this.records = new ArrayList();
		}
	}
	
	public Page(String table){
		this.tableName = table;
		this.storeKey = table;
		this.records = new ArrayList();
	}
	
	
	public Page(String table, String col){
		this.tableName = table;
		this.colName = col;
		this.storeKey = table+"-"+col;
		this.records = new ArrayList();
	}
	
	public void setTimestamp(){
		this.ts = System.nanoTime();
	}
	
	public void clearPage(){
		this.tableName = null;
		this.colName = null;
		this.storeKey = null;
		this.records = null;
		this.ts = 0;
	}
	
	public void setToBeDeleted(int id){
		this.isDeleted = true;
		this.deleteID = id;
	}
	
	public void undoDelete(){
		this.isDeleted = false;
		this.deleteID = -1;
	}
	
	public int getDeleteLogID(){
		return this.deleteID;
	}
	
	public boolean checkIfDeleted(){
		return this.isDeleted;
	}
	

}