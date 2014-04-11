package heapFile;

import java.io.IOException;

import bufmgr.BufMgr;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import chainexception.ChainException;
import global.*;
import heap.*;
import diskmgr.*;

public class Heapfile {
	private HFPage page;

	public Heapfile(String string) throws FileNameTooLongException,
			InvalidPageNumberException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException, FileIOException,
			DiskMgrException, IOException {
		SystemDefs.JavabaseDB.openDB(SystemDefs.JavabaseDBName);
		// TODO Auto-generated constructor stub
		PageId pid = new PageId();
		if(SystemDefs.JavabaseDB.get_file_entry(string) == null)
		{
			SystemDefs.JavabaseDB.add_file_entry(string, pid);
			System.out.println(pid);
//			page.setNextPage(pid);
		}
		else
		{
			pid = (SystemDefs.JavabaseDB.get_file_entry(string));
			System.out.println(pid);
//			page.setCurPage(pid);
		}
	}

	public Scan openScan() {
		// TODO Auto-generated method stub
		return null;
	}

	public RID insertRecord(byte[] byteArray) throws ChainException {
		// TODO Auto-generated method stub
		Page page;
		return null;
	}

	public int getRecCnt() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean deleteRecord(RID rid) {
		// TODO Auto-generated method stub
		boolean found = false;
		try {
			while (!found) {
				PageId temp = page.getNextPage();
				if(temp == null)
					break;
				SystemDefs.JavabaseBM.pinPage(temp, page, true);
				page.setCurPage(temp);
				RID firstRec = page.firstRecord();
				while(firstRec!=null)
				{
					firstRec = page.nextRecord(firstRec);
					if(firstRec == null)
						break;
					if(firstRec == rid)
					{
						found = true;
						page.deleteRecord(rid);
						break;
					}
				}
				SystemDefs.JavabaseBM.unpinPage(temp, true);
			}
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufferPoolExceededException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PagePinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return found;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException {
		// TODO Auto-generated method stub
		return false;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		return null;
	}

}
