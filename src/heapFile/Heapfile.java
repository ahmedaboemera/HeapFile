package heapFile;

import java.io.IOException;
import java.util.Arrays;

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
	private PageId firstPid;

	public Heapfile(String string) throws FileNameTooLongException,
			InvalidPageNumberException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException, FileIOException,
			DiskMgrException, IOException {
		page = new HFPage();
		recCnt = 0;
		SystemDefs.JavabaseDB.openDB(SystemDefs.JavabaseDBName);
		// TODO Auto-generated constructor stub
		firstPid = new PageId();
		if (SystemDefs.JavabaseDB.get_file_entry(string) == null) {
			SystemDefs.JavabaseDB.add_file_entry(string, firstPid);
			page.init(firstPid, new Page());
			page.setCurPage(firstPid);
			page.setNextPage(new PageId(-1));
		} else {
			firstPid = (SystemDefs.JavabaseDB.get_file_entry(string));
			page.setCurPage(firstPid);
		}
	}

	public Scan openScan() {
		// TODO Auto-generated method stub
		try {
			page.setCurPage(firstPid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scan scan = new Scan(page);
		return scan;
	}

	public RID insertRecord(byte[] byteArray) throws ChainException {
		// TODO Auto-generated method stub
		boolean go = true;
		PageId idOfLastPage = new PageId();
		while (true) {
			try {
				if (page != null && page.getNextPage().pid == -1) {

					idOfLastPage.copyPageId(page.getCurPage());
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Page tempage = new Page(page.getpage());
				SystemDefs.JavabaseBM.pinPage(page.getCurPage(), tempage, true);
				if (page.available_space() >= byteArray.length) {
					go = false;
					RID newRid = new RID();
					newRid = page.insertRecord(byteArray);
					SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), true);
					recCnt++;
//					page.setCurPage(firstPid);
					return newRid;

				} else {

					SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
					PageId newPageId = page.getNextPage();
					if (newPageId.pid == -1)
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
				page.init(newPageId, new Page());
				page.setCurPage(newPageId);
				if (page.available_space() >= byteArray.length) {
					RID newRid = new RID();
					newRid = page.insertRecord(byteArray);
					newRid.pageNo = newPageId;
					page.setPrevPage(idOfLastPage);
					page.setCurPage(idOfLastPage);
					page.setNextPage(newPageId);
					page.setCurPage(newPageId);
					page.setNextPage(new PageId(-1));
					SystemDefs.JavabaseBM.unpinPage(newPageId, true);
					recCnt++;
//					page.setCurPage(firstPid);
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
		return recCnt;
	}

	public boolean deleteRecord(RID rid) {
		// TODO Auto-generated method stub
		boolean found = false;
		try {
			while (!found) {
				PageId temp = page.getNextPage();
				if (temp == null)
					break;
				SystemDefs.JavabaseBM.pinPage(temp, page, true);
				page.setCurPage(temp);
				RID firstRec = page.firstRecord();
				while (firstRec != null) {
					if (firstRec == rid) {
						found = true;
						page.deleteRecord(rid);
						SystemDefs.JavabaseBM.unpinPage(temp, true);
						return true;
					}
					firstRec = page.nextRecord(firstRec);
				}
				SystemDefs.JavabaseBM.unpinPage(temp, true);
			}
		} catch (IOException e) {
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

		// / dont forget to check that the insert method returns the pointer to
		// the start of the linked list

		HFPage traverse = new HFPage();
		traverse = page;
		while (true) {

			try {
				SystemDefs.JavabaseBM.pinPage(traverse.getCurPage(), traverse,
						true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				if (traverse.getRecord(rid) != null) {

					Tuple tuple = traverse.getRecord(rid);
					if (tuple.getLength() == newTuple.getLength()) {
						tuple.tupleCopy(newTuple);
						SystemDefs.JavabaseBM.unpinPage(traverse.getCurPage(),
								true);
						return true;

					} else {
						throw new InvalidUpdateException(null, "heap.InvalidUpdateException");
						// throw the exception here aboemera :)
						// I left this part for you because of the problems I
						// have with the jar
						// the exception to be thrown is InvalidUpdateException
						// --- > can be found in Test 4
					}

				} else {
					SystemDefs.JavabaseBM.unpinPage(traverse.getCurPage(),
							false);
					PageId newPageId = traverse.getNextPage();
					if (newPageId.pid == -1) {
						break;
					}
					traverse.setCurPage(newPageId);

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return false;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		try {
			page.setCurPage(firstPid);
			while (page.getCurPage().pid != -1) {
				Page myPage = new Page(page.getHFpageArray());
				SystemDefs.JavabaseBM.pinPage(page.getCurPage(), myPage, false);
				RID frid = page.firstRecord();
				while(!frid.equals(null)){
					if(frid.equals(rid))
					{
						Tuple inst = page.getRecord(rid);
						SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
						return inst;
					}
					frid = page.nextRecord(frid);
				}
				SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
				page.setCurPage(page.getNextPage());
			}
		} catch (IOException e) {
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
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
