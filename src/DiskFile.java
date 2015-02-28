import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DiskFile {
	protected String filePath = null;
	protected int[] header = null;

	private static int BUCKET_NUM = 16;
	RandomAccessFile file = null;

	public DiskFile(String filePath) {
		try {
			this.file = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e) {
			System.err.println("No such file exist");
			e.printStackTrace();
		}
		this.filePath = filePath;

		this.header = new int[BUCKET_NUM];
		this.readFileHeader();
	}

	public void createNewPageToHoldTupleId(Tuple tuple) {
		// TODO Auto-generated method stub
		int id = tuple.getId();
		if (header[id % BUCKET_NUM] == -1) {
			header[id % BUCKET_NUM] = this.getFileLength();
			this.writeFileHeader();
		}
		DiskPage newPage = new DiskPage();
		newPage.appendInt(id);
		this.appendNewPageToFile(newPage);
	}

	public void createNewPageToHoldTupleName(Tuple tuple) {
		int id = tuple.getId();
		if (header[id % BUCKET_NUM] == -1) {
			header[id % BUCKET_NUM] = this.getFileLength();
			this.writeFileHeader();
		}
		String name = tuple.getName();
		DiskPage newPage = new DiskPage();
		newPage.appendString(name, 16);
		this.appendNewPageToFile(newPage);
	}

	public void createNewPageToHoldTuplePhone(Tuple tuple) {
		int id = tuple.getId();
		if (header[id % BUCKET_NUM] == -1) {
			header[id % BUCKET_NUM] = this.getFileLength();
			this.writeFileHeader();
		}
		String phone = tuple.getPhone();
		DiskPage newPage = new DiskPage();
		newPage.appendString(phone, 12);
		this.appendNewPageToFile(newPage);
	}

	public void appendNewPageToFile(DiskPage page) {
		writeToFileEnd(page.getData());
	}

	public int getFileLength() {
		int length = 0;
		try {
			length = (int) file.length();
		} catch (IOException e) {
			System.err.print("Error writing to the end of file");
		}
		return length;
	}

	private void writeToFileEnd(byte[] data) {
		try {
			file.seek(file.length());
			file.write(data);
		} catch (IOException e) {
			System.err.print("Error writing to the end of file");
		}
	}

	private byte[] readFromFile(int position, int size) {
		byte[] bytes = null;
		try {
			file.seek(position);
			bytes = new byte[size];
			file.read(bytes);
		} catch (IOException e) {
			System.err.println("Error reading from file");
		}
		return bytes;
	}

	public void readFileHeader() {
		ByteBuffer headerBytes = ByteBuffer.allocate(4 * 16);
		headerBytes.put(readFromFile(0, 16 * 4));
		for (int i = 0; i < this.header.length; i++)
			this.header[i] = headerBytes.getInt();
	}

	// private byte[] readPageWithPagePointer(int id) {
	// int pagePointer = this.pagePointers[id % BUCKET_NUM];
	// if (pagePointer < 0) {
	// System.err.println("pagePointer must be a valid page index number");
	// System.exit(-1);
	// }
	// int pageOffsetByte = 0 + 16 * 4 + pagePointer * PAGE_SIZE;
	// return readFromFile(pageOffsetByte, PAGE_SIZE);
	// }

	private void writeFileHeader() {
		try {
			ByteBuffer headerBytes = ByteBuffer.allocate(4 * 16);
			for (int i = 0; i < this.header.length; i++)
				headerBytes.putInt(this.header[i]);
			byte[] data = headerBytes.array();
			file.seek(0);
			file.write(data);
		} catch (IOException e) {
			System.err.print("Error writing to the end of file");
		}
	}


}
