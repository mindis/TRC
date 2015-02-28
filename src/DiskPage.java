import java.nio.ByteBuffer;
import java.util.Arrays;

public class DiskPage {
	private static int PAGE_SIZE = 512; // 512B
	private byte[] pages;
	// the last int is a pointer to next page, initialized to be -1
	// the second last int is a pointer to next available slot.
	public DiskPage() {
		pages = new byte[PAGE_SIZE];
		setNextPagePointer(-1);//no next pages initially
		setCursorPointer(0);//no element in the current page 
	}
	
	public DiskPage(byte[] pages){
		this.pages = pages;
	}
	
	public boolean isFull(){
		return getCursorPointer() >= PAGE_SIZE - 8;
	}
	
	private void setCursorPointer(int value) {
		// may to inconsistency
		byte[] intByte = ByteManager.intToBytes(value);
		for (int i = 0; i < intByte.length; i++)
			pages[PAGE_SIZE - 4 + i] = intByte[i];
	}

	private void setNextPagePointer(int value) {
		byte[] intByte = ByteManager.intToBytes(value);
		for (int i = 0; i < intByte.length; i++)
			pages[PAGE_SIZE - 8 + i] = intByte[i];
	}
	
	private int getNextPagePointer(){
		byte[] intByte = Arrays.copyOfRange(pages, PAGE_SIZE - 8, PAGE_SIZE - 4);
		return ByteManager.bytesToInt(intByte);
	}
	
	private int getCursorPointer(){
		byte[] intByte = Arrays.copyOfRange(pages, PAGE_SIZE - 4, PAGE_SIZE );
		return ByteManager.bytesToInt(intByte);
	}

	public byte[] getData() {
		return this.pages;
	}

	public void appendInt(int x) {
		// TODO check if reach the end of the page
		int curPos = getCursorPointer();
		if (curPos + 4 > PAGE_SIZE - 8) {
			System.err.println("page shouldn't overflow");
		}
		byte[] intByte = ByteManager.intToBytes(x);
		for (int i = 0; i < intByte.length; i++)
			pages[curPos + i] = intByte[i];
		setCursorPointer(curPos + 4);
	}

	public void appendString(String str, int strlen) {
		int curPos = getCursorPointer();
		if (curPos + strlen > PAGE_SIZE - 8) {
			System.err.println("page shouldn't overflow");
		}
		// TODO check if reach the end of the page
		byte[] strByte = ByteManager.strToBytes(str, strlen);
		for (int i = 0; i < strByte.length; i++)
			pages[curPos + i] = strByte[i];
		setCursorPointer(curPos + strlen);
	}

}
