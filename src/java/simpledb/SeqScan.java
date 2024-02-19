package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;

    private int table_id;
    private String table_alias;
    private TupleDesc td;
    private DbFileIterator tuple_iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.table_id = tableid;
        this.table_alias = tableAlias;

        DbFile db_file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = db_file.getTupleDesc();
        this.tuple_iterator = db_file.iterator(this.tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.table_id);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return this.table_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.table_id = tableid;
        this.table_alias = tableAlias;

        DbFile db_file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = db_file.getTupleDesc();
        this.tuple_iterator = db_file.iterator(this.tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.tuple_iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        int field_num = this.td.numFields();
        Type[] field_types = new Type[field_num];
        String[] field_names = new String[field_num];

        for (int i = 0; i < field_num; i++) {
            field_types[i] = td.getFieldType(i);
            field_names[i] = this.table_alias + '.' + this.td.getFieldName(i);
        }
        return new TupleDesc(field_types, field_names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.tuple_iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return this.tuple_iterator.next();
    }

    public void close() {
        // some code goes here
        this.tuple_iterator.close();
    }

    // Rewind the stream back to the start. 
    // This could be necessary for implementing e.g. nested loop joins.
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        close();
        open();
    }
}
