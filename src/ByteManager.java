import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class ByteManager {
	public static byte[] intToBytes(int x){
		return ByteBuffer.allocate(4).putInt(x).array();
	}
	
	public static int bytesToInt(byte[] bytes){
		return new BigInteger(bytes).intValue();
	}
	
	public static byte[] strToBytes(String str, int fixedSize){
		byte[] bytes = new byte[fixedSize];
		bytes = Arrays.copyOf(str.getBytes(), fixedSize);
		return bytes;
	}
}
