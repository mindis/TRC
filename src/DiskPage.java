import java.nio.ByteBuffer;
import java.util.Arrays;

public class DiskPage {
	private static int PAGE_SIZE = 512;//512

	public DiskPage() {
		
	}
	
	public byte[] pageToBytes(){
		byte[] tupleBytes = new byte[32];
		byte[] nameArr = new byte[16];
		byte[] phoneArr = new byte[12];
		nameArr = Arrays.copyOf(this.name.getBytes(), 16);
		phoneArr = Arrays.copyOf(this.phone.getBytes(), 16);
		ByteBuffer tupleBuf = ByteBuffer.allocate(32);
		tupleBuf.putInt(id);
		tupleBuf.put(nameArr);
		tupleBuf.put(phoneArr);
		return tupleBuf.array();
	}

	public int getId() {
		// TODO Auto-generated method stub
		return this.id;
	}
	
}
