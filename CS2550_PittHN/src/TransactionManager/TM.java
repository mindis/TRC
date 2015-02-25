package TransactionManager;

import java.util.ArrayList;
import java.util.Random;

import Common.Instruction;

public class TM {

	public int currentIndex=0;
	public Instruction loadNext(){
		if(this.hasNextInstr()){
			Instruction x=instructionList.get(this.currentIndex);
			this.currentIndex++;
			return x;
		}else{
			return null;
		}
		
	}
	public boolean hasNextInstr(){
		return (this.currentIndex<instructionList.size());
	}
	
	public static StringBuffer TMLogger = new StringBuffer();
	public static int randomSeed = 30;
	public static int concurrentReadOption =1;  //1 for round robin and 2 for random
	
	public static String[] codes = null;
	
	public static ArrayList<Instruction> instructionList = new ArrayList<Instruction>();

	public static void execute() {

		ArrayList<ScriptFile> fileList = new ArrayList();
		for (int i = 0; i < codes.length; i++) {
			TMLogger.append("Reading script files.\n");
			ScriptFile sf = new ScriptFile(i, codes[i].trim());
			if (sf.getInstructions()) {
				fileList.add(sf);
			} else {
				System.out
						.println("Warning: invalid script format of the file "
								+ i + ". This file will be ignored");
				TMLogger.append("Warning: invalid script format of the file "
						+ i + ". This file will be ignored. \n");
			}
		}
		System.out.println("There are " + fileList.size()
				+ " script files to be run.");
	
	
	if(fileList.size()<1){
		//System.exit(0);
	}
	
	/*
	 * Round robin
	 */
	if(concurrentReadOption==1){ //Round Robin
		int counter = 0;
		while(fileList.size()>0){
			ScriptFile sf = fileList.get(counter);
			if(!sf.instructions.isEmpty()){
				instructionList.add(sf.instructions.remove());
			}else{
				fileList.remove(counter);
			}
			counter++;
			if(counter>=fileList.size()){
				counter=0;
			}
		}
		
	}else{ //Random reading
		int unFinishedFiles = fileList.size();
		Random rndMubers = new Random(randomSeed);
		Math.abs(rndMubers.nextInt());
		while(unFinishedFiles>0){
			int counter = Math.abs(rndMubers.nextInt())%unFinishedFiles;
			ScriptFile sf = fileList.get(counter);
			if(!sf.instructions.isEmpty()){
				int readSize = Math.abs(rndMubers.nextInt())%sf.instructions.size()+1;
				for(int i=0;i<readSize;i++){
					instructionList.add(sf.instructions.remove());
				}
				
			}else{
				fileList.remove(counter);
				unFinishedFiles--;
			}
		}
	}
	
	//TEST TODO
	for(int i=0;i<instructionList.size();i++){
		Instruction inst = instructionList.get(i);
		System.out.println(inst.fileID+"-"+inst.tidInFile+": "+inst.command+" "+inst.table+" "+inst.data);
	}
	//TEST END
	

}
}
