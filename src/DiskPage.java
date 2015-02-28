import java.nio.ByteBuffer;


public class DiskPage {
	private static int PAGE_SIZE = 512; // 512B
	private byte[] page;
	// the last int is a pointer to next page, initialized to be -1
	// the second last int is a pointer to next available slot.
	public DiskPage() {
		page = new byte[PAGE_SIZE];
		putInt(-1, PAGE_SIZE - 4);
		putInt(0, PAGE_SIZE - 8);
	}
	
	public byte[] getData() {
		return this.page;
	}
	
	public void putInt(int x, int position, boolean append) {
		byte[] intByte = ByteBuffer.allocate(4).putInt(x).array();
		for (int i = 0; i < intByte.length; i++)
			page[position + i] = intByte[i];
	}
	
}
