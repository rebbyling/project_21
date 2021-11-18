package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import javax.imageio.IIOException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.id;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        try {
            //create an empty page
            // if (pid.getPageNumber() == numPages()) {
            //     Page page = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            //     writePage(page);
            //     return page;
            // } else {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.f, "r");
            randomAccessFile.seek(BufferPool.getPageSize() * pid.getPageNumber());
            byte[] data = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(data);
            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
            // }
        }catch (IllegalArgumentException | IOException e) {
            throw new IllegalArgumentException(e);
        }
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageArr = new ArrayList<>();
        //for existing pages, loop through to find an empty slot in page to insert the tuple
        for (int i=0; i<numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) { //if there are empty slots in page, insert tuple
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                pageArr.add(page);
                break;
            }

        }
        //if there are no existing pages, create a new page and add in the tuple
        if (pageArr.isEmpty()) {
            HeapPage newPg = new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]);
            newPg.insertTuple(t);
            this.writePage(newPg);
            pageArr.add(newPg);
        }
        return pageArr;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // retrieve page id and heap page
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                                                                    Permissions.READ_WRITE);

        // delete tuple from heap page
        page.deleteTuple(t);
        
        // return page array
        ArrayList<Page> pageArray = new ArrayList<>();
        pageArray.add(page);
        
        return pageArray;
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }


    private class HeapFileIterator implements DbFileIterator  {
        
        private TransactionId tid;
        // private HeapPageId pid;
        // private HeapPage heapPage;
        private int pageNo = 0;
        private Iterator<Tuple> iter;
        
        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
            
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            pageNo = 0;
            // this.pid = new HeapPageId(getId(), pageNo);
            // this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid,
            //                                                             Permissions.READ_ONLY);
            // this.iter = heapPage.iterator();
            this.iter = openPage(pageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if (this.iter == null) {
                return false;
            }

            while (!this.iter.hasNext()) {
                ++pageNo;
                if (pageNo >= numPages()) {
                    return false;
                }
                this.iter = openPage(pageNo);
            }
            return true;
        }

        private Iterator openPage(int pageNo) throws NoSuchElementException, TransactionAbortedException, DbException {
            HeapPage pg;
            pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY);
            return pg.iterator();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if (hasNext()){
                return this.iter.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            pageNo = 0;
            this.iter = openPage(pageNo);
            
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            this.pageNo = 0;
            this.tid = null;
            // this.pid = null;
            // this.heapPage = null;
            this.iter = null;
        }

    }

}