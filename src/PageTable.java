public class PageTable {
	private int entryNum;
	private PTE[] entries;

	public PageTable(int entryNum) {
		this.entryNum = entryNum;
		// all the entries are invalid initially
		this.entries = new PTE[entryNum];
	}

	class PTE {
		boolean isValid, isDirty;
		int memPageNum;
		int bucketNum;
		int idPageNum, namePageNum, phonePageNum;

		public PTE() {
			this.isValid = false;
		}

		public void setPTE(boolean isValid, boolean isDirty, int memPageNum,
				int bucketNum, int idPageNum, int namePageNum, int phonePageNum) {
			this.isValid = isValid;
			this.isDirty = isDirty;
			this.memPageNum = memPageNum;
			this.bucketNum = bucketNum;
			this.idPageNum = idPageNum;
			this.namePageNum = namePageNum;
			this.phonePageNum = phonePageNum;
		}

	}
}
