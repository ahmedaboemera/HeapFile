package heapFile;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;

public class Scan {
	private  HFPage curHFpage;
	private PageId firstpid;

	public Scan(HFPage now) {
		// TODO Auto-generated constructor stub
		this.curHFpage = now;
		try {
			firstpid = now.getCurPage();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			Page myPage = new Page(now.getHFpageArray());
			SystemDefs.JavabaseBM.pinPage(now.getCurPage(), myPage, false);
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public Tuple getNext(RID rid) {
		// TODO Auto-generated method stub
		boolean found = false;
		try {
			while (!found) {
				PageId temp = curHFpage.getCurPage();
					if(temp.pid == -1)
						break;
					Page tempage = new Page(curHFpage.getHFpageArray());
					SystemDefs.JavabaseBM.pinPage(temp, tempage, false);
					RID firstRid = curHFpage.firstRecord();
					while(curHFpage.nextRecord(firstRid) != null)
					{
						if(firstRid.equals(rid))
						{
							SystemDefs.JavabaseBM.unpinPage(temp, false);
							Tuple ret = curHFpage.getRecord(curHFpage.nextRecord(firstRid));
							rid = curHFpage.nextRecord(rid);
							return ret;
						}
						firstRid = curHFpage.nextRecord(firstRid);
						
					}
					SystemDefs.JavabaseBM.unpinPage(temp, false);
					curHFpage.setCurPage(curHFpage.getNextPage());
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

	public void closescan() {
		// TODO Auto-generated method stub
		try {
			this.finalize();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
