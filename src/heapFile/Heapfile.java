package heapFile;

import java.io.IOException;

import chainexception.ChainException;
import global.*;
import heap.*;
import diskmgr.*;

public class Heapfile {

	public Heapfile(String string) throws FileNameTooLongException, InvalidPageNumberException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, FileIOException, DiskMgrException, IOException {
		// TODO Auto-generated constructor stub
		DB a = new DB();
		a.add_file_entry(string, new PageId());
	}

	public Scan openScan() {
		// TODO Auto-generated method stub
		return null;
	}

	public RID insertRecord(byte[] byteArray) throws ChainException{
		// TODO Auto-generated method stub
		return null;
	}

	public int getRecCnt() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean deleteRecord(RID rid) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException{
		// TODO Auto-generated method stub
		return false;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		return null;
	}

}
