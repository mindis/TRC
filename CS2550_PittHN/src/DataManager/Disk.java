package DataManager;

import java.util.ArrayList;
import java.util.HashMap;

public class Disk {
	public static int DISK_SIZE = 536870912; //Initially 500MB disk size, auto expandable on demand
	
	public static HashMap<String, ArrayList> tablePages = new HashMap<String, ArrayList>(); 
	
	public static int[] pageAvailability = new int[Disk.DISK_SIZE/DM.PAGE_SIZE];// 0:empty, 1: not full, 2:full
	
	public static Page[] diskPages = new Page[Disk.DISK_SIZE/DM.PAGE_SIZE];
	
}
