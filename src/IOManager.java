import java.io.IOException;
import java.io.RandomAccessFile;


public class IOManager {
	public static byte[] readFromFile(String filePath, int position, int size) {
		byte[] bytes = null;
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "r");
			file.seek(position);
			bytes = new byte[size];
			file.read(bytes);
			file.close();
		} catch (IOException e) {
			System.err.println("Error reading from file");
		}
		return bytes;

	}
	
	public static void writeToFileEnd(String filePath, byte[] data) {
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "rw");
			file.seek(file.length());
			file.write(data);
			file.close();
		} catch (IOException e) {
			System.err.print("Error writing to the end of file");
		}
	}

	public static void writeToFile(String filePath, byte[] data, int position) {
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "rw");
			file.seek(position);
			file.write(data);
			file.close();
		} catch (IOException e) {
			System.err.print("Error writing to file");
		}
	}
}
