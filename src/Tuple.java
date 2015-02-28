import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Tuple {
	int id;
	String name, phone;
	private static final int TUPLE_SIZE = 32;

	public Tuple(String tupleStr) {
		// tokenize the tuplestr
		tupleStr = tupleStr.substring(1, tupleStr.length() - 1);
		String[] tokens = tupleStr.split(",\\s");
		this.id = Integer.parseInt(tokens[0]);
		this.name = tokens[1];
		this.phone = tokens[2];
	}

	public Tuple(int id, String name, String phone) {
		this.id = id;
		this.name = name;
		this.phone = phone;
	}

	public byte[] tupleToBytes() {
		byte[] nameArr = new byte[16];
		byte[] phoneArr = new byte[12];
		nameArr = Arrays.copyOf(this.name.getBytes(), 16);
		phoneArr = Arrays.copyOf(this.phone.getBytes(), 12);
		ByteBuffer tupleBuf = ByteBuffer.allocate(32);
		tupleBuf.putInt(id);
		tupleBuf.put(nameArr);
		tupleBuf.put(phoneArr);
		return tupleBuf.array();
	}

	public static Tuple byteToTuple(byte[] bytes) {
		byte[] idArr = Arrays.copyOfRange(bytes, 0, 4);
		byte[] nameArr = Arrays.copyOfRange(bytes, 4, 20);
		byte[] phoneArr = Arrays.copyOfRange(bytes, 20, 32);
		int id = new BigInteger(idArr).intValue();
		String name = new String(nameArr);
		String phone = new String(phoneArr);
		return new Tuple(id, name, phone);
	}

	public int getId() {
		return this.id;
	}

	public String toString() {
		return "[id: " + this.id + "][name: " + this.name + "][phone: "
				+ this.phone + "]";
	}

	public static void main(String[] args) {
		Tuple tuple = new Tuple(12, "Qihang", "412-43342");
		System.out.println(tuple);
		byte[] tupleBytes = tuple.tupleToBytes();
		System.out.println(Tuple.byteToTuple(tupleBytes));
	}
}
