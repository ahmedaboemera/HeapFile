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
	private int recCnt;
	public Heapfile(String string) throws FileNameTooLongException,
			InvalidPageNumberException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException, FileIOException,
			DiskMgrException, IOException {
		page = new HFPage();
		recCnt = 0;
		SystemDefs.JavabaseDB.openDB(SystemDefs.JavabaseDBName);
		// TODO Auto-generated constructor stub
		PageId pid = new PageId();
		if(SystemDefs.JavabaseDB.get_file_entry(string) == null)
		{
			SystemDefs.JavabaseDB.add_file_entry(string, pid);
			page.setCurPage(pid);
		}
		else
		{
			pid = (SystemDefs.JavabaseDB.get_file_entry(string));
			page.setCurPage(pid);
		}
	}

	public Scan openScan() {
		// TODO Auto-generated method stub
		return new Scan(page);
	}

	public RID insertRecord(byte[] byteArray) throws ChainException {
		// TODO Auto-generated method stub
		boolean go = true;
		PageId idOfLastPage = new PageId();

		while (true) {

			try {
				if (page != null && page.getNextPage() == null) {

					idOfLastPage.copyPageId(page.getCurPage());

				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {

				SystemDefs.JavabaseBM.pinPage(page.getCurPage(), page, true);
				if (page.available_space() >= byteArray.length) {

					go = false;
					RID newRid = new RID();
					newRid = page.insertRecord(byteArray);

					SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), true);
					recCnt ++;
					return newRid;

				} else {
					SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
					PageId newPageId = page.getNextPage();
					if (newPageId == null)
						break;
					page.setCurPage(newPageId);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		if (go) {

			try {
				PageId newPageId = SystemDefs.JavabaseBM.newPage(new Page(), 1);
				page.setCurPage(newPageId);
				if (page.available_space() >= byteArray.length) {
					RID newRid = new RID();
					newRid = page.insertRecord(byteArray);
					newRid.pageNo = newPageId;
					page.setPrevPage(idOfLastPage);
					page.setCurPage(idOfLastPage);
					page.setNextPage(newPageId);
					SystemDefs.JavabaseBM.unpinPage(newPageId, true);
					recCnt++;
					return newRid;
				}
				// throw exception

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return null;

		// this return statement has no use at all but removing the error from
		// the method ... we may need to change this later ;

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
					if(firstRec == rid)
					{
						found = true;
						page.deleteRecord(rid);
						SystemDefs.JavabaseBM.unpinPage(temp, true);
						return true;
					}
					firstRec = page.nextRecord(firstRec);
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
