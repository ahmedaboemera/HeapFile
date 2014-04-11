package heapFile;

import java.io.IOException;

import sun.text.normalizer.CharTrie.FriendAgent;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class Scan {
	private HFPage curHFpage;

	public Scan(HFPage now) {
		// TODO Auto-generated constructor stub
		this.curHFpage = now;
	}

	public Tuple getNext(RID rid) {
		// TODO Auto-generated method stub
		boolean found = false;
		try {
			while (!found) {
					PageId temp = curHFpage.getNextPage();
					if(temp == null)
						break;
					SystemDefs.JavabaseBM.pinPage(temp, curHFpage, false);
					RID firstRid = curHFpage.firstRecord();
					while(firstRid != null)
					{
						if(firstRid == rid)
						{
							SystemDefs.JavabaseBM.unpinPage(temp, false);
							Tuple ret = curHFpage.getRecord(firstRid);
							return ret;
						}
						firstRid = curHFpage.nextRecord(firstRid);
						
					}
					SystemDefs.JavabaseBM.unpinPage(temp, false);
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
