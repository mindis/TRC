package TransactionManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import Common.Instruction;

public class ScriptFile {
	public int tCounter = 0;
	public int fileID;
	public String filename;
	public Queue<Instruction> instructions;
	
	public ScriptFile(int id, String name){
		this.fileID = id;
		this.filename = name;
		this.instructions = new LinkedList();
	}
	
	public boolean getInstructions(){
		File file = new File(filename);
		if (file.exists()&&file.isFile()) {
			Instruction inst = null;
				BufferedReader reader = null;
					try {
						reader = new BufferedReader(new FileReader(file));
						String text;
						int counter = 0;
						while ((text = reader.readLine())!= null) {
							counter++;
							text = text.trim();
							if(text.length()>0){
								if(text.startsWith("C") || text.startsWith("A")){
									inst = new Instruction(this.fileID, tCounter, text, text,"","");
									
								}else if(text.startsWith("B") || text.startsWith("D")){
									if(text.startsWith("B"))
										this.tCounter++;
									String[] parts = text.split(" ");
									if(parts.length==2){
										inst = new Instruction(this.fileID, tCounter, text, parts[0].trim(),parts[1].trim(),"");
									}else{
										System.out.println("[Transaction Manager] Script format error 1 at line "+counter+".");
										return false;
									}
									
								}else if(text.startsWith("R")||text.startsWith("M")||text.startsWith("G")){
									String[] parts = text.split(" ");
									if(parts.length==3){
										inst = new Instruction(this.fileID,tCounter, text, parts[0].trim(),parts[1].trim(),parts[2].trim());
									}else{
										System.out.println("[Transaction Manager] Script format error 2 at line "+counter+".");
										return false;
									}
									
								}else if(text.startsWith("W")){
									String[] parts = text.split("\\(");
									if(parts.length==2){
										String[] tmps = parts[0].split(" ");
										if(tmps.length==2){
											inst = new Instruction(this.fileID, tCounter, text, tmps[0].trim(),tmps[1].trim(),parts[1].trim().substring(0,parts[1].trim().length()-1));
										}else{
											
										}
									}else{
										System.out.println("[Transaction Manager] Script format error 3 at line "+counter+".");
										return false;
									}
									
								}else{
									System.out.println("[Transaction Manager] Script format error 3 at line "+counter+".");
									return false;
								}
								this.instructions.add(inst);
							}
						}
					} catch (IOException e) {
						System.out.println("[Transaction Manager] Cannot read the file.");
						e.printStackTrace();
					}				
					try {
						reader.close();
					} catch (IOException e) {
						System.out.println("[Transaction Manager] Cannot read the file.");
						e.printStackTrace();
					}	
					return true;
		}else{
			return false;
		}
	}

}
