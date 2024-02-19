package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int num_page;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        int page_size = BufferPool.getPageSize(); 
        this.num_page = (int) (f.length() * 1.0 / page_size);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        int page_size = BufferPool.getPageSize();
        byte[] data = new byte[page_size];
        try {
            RandomAccessFile r = new RandomAccessFile(this.file, "r");
            int offset = pid.pageNumber() * page_size;
            if ((offset + page_size) > r.length()) {
                throw new IllegalArgumentException();
            }

            r.seek(offset);
            r.read(data);
            r.close();

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
    	// not necessary for this assignment
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.num_page;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	// not necessary for this assignment
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	// not necessary for this assignment
        return null;
    }

    // Returns a HeapFileIterator
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}



/**
 * Helper class that implements the Java Iterator for tuples on
 * a HeapFile
 */
class HeapFileIterator implements DbFileIterator {
    Iterator<Tuple> it = null;
    int curpgno;

    TransactionId tid;
    HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open()
        throws DbException, TransactionAbortedException {
        // Some Code Here
        this.curpgno = 0;
        this.it = openPage(this.curpgno).iterator();
    }

    private HeapPage openPage(int page_no) throws DbException, TransactionAbortedException {

        HeapPageId pid = new HeapPageId(this.hf.getId(), page_no);
        return (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // Some Code Here
        if (this.it == null) {
            return false;
        }
        // the current page has remaining tuples.
        if (this.it.hasNext()) {
            return true;
        }
        // iterate to the next page
        if (this.curpgno < this.hf.numPages() - 1) {
            this.curpgno += 1;
            // System.out.println(this.curpgno + "cur page no");
            // System.out.println(this.hf.numPages() + "total page");
            this.it = openPage(this.curpgno).iterator();
            return this.it.hasNext();
        }
        return false;
    }

    // Return the next tuple in the HeapFile
    public Tuple next() throws TransactionAbortedException, DbException {
        // Some Code Here
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return this.it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException{
        // Not needed for this assignment
        close();
        open();
    }

    public void close() {
        // Some Code Here
        this.curpgno = 0;
        this.it = null;
    }
}


