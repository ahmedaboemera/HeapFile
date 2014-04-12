package heapFile;

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

	public Scan(HFPage now) {
		// TODO Auto-generated constructor stub
		this.curHFpage = now;
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
		try {
			curHFpage.setCurPage(rid.pageNo);
			RID frid = curHFpage.firstRecord();
			Page mypage = new Page(curHFpage.getHFpageArray());
			SystemDefs.JavabaseBM.pinPage(rid.pageNo, mypage, false);
			while(frid!= null){
				if(frid.equals(rid))
				{
					if(curHFpage.nextRecord(rid) == null)
					{
						SystemDefs.JavabaseBM.unpinPage(frid.pageNo, false);
						curHFpage.setCurPage(curHFpage.getNextPage());
						Page mynewpage = new Page(curHFpage.getHFpageArray());
						SystemDefs.JavabaseBM.pinPage(curHFpage.getCurPage(), mynewpage	, false);
						rid = curHFpage.firstRecord();
						Tuple inst = curHFpage.getRecord(rid);
						SystemDefs.JavabaseBM.unpinPage(curHFpage.getCurPage(), false);
						return inst;
					}
					rid = curHFpage.nextRecord(rid);
					Tuple inst = curHFpage.getRecord(rid);
					SystemDefs.JavabaseBM.unpinPage(curHFpage.getCurPage(), false);
					return inst;
				}
				frid = curHFpage.nextRecord(frid);
			}
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
