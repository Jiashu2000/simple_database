package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    // arraylist to store TDItems
    private final ArrayList<TDItem> td_list;
    // keep size of current td_list
    private int td_size = 0;


    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return td_list.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.td_list = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length && i < fieldAr.length; i++) {
            this.td_list.add(new TDItem(typeAr[i], fieldAr[i]));
            this.td_size += typeAr[i].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.td_list = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            this.td_list.add(new TDItem(typeAr[i], null));
            this.td_size += typeAr[i].getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.td_list.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= 0 && i < this.td_list.size()) {
            return this.td_list.get(i).fieldName;
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= 0 && i < this.td_list.size()) {
            return this.td_list.get(i).fieldType;
        }
        throw new NoSuchElementException();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        String fname;
        for (int i = 0; i < this.td_list.size(); i++) {
            fname = this.td_list.get(i).fieldName;
            if (fname != null && fname.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        /* 
        int td_size = 0;
        for (int i = 0; i < this.td_list.size(); i++) {
            td_size += this .td_list.get(i).fieldType.getLen();
        }
        return td_size;
        */
        // keep a int instead of loop over list every time. update the
        return this.td_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int tot_fields = td1.numFields() + td2.numFields();
        Type[] type_arr = new Type[tot_fields];
        String[] name_arr = new String[tot_fields];
        int i = 0;
        int n = td1.numFields();

        for (; i < n; i++) {
            type_arr[i] = td1.td_list.get(i).fieldType;
            name_arr[i] = td1.td_list.get(i).fieldName;
        }

        for (; i < tot_fields; i++) {
            type_arr[i] = td2.td_list.get(i-n).fieldType;
            name_arr[i] = td2.td_list.get(i-n).fieldName;
        }

        return new TupleDesc(type_arr, name_arr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            TupleDesc o_td = (TupleDesc) o;
            if (o_td.numFields() != this.numFields()) {
                return false;
            }
            if (o_td.td_size != this.td_size) { 
                return false;
            } 
            for (int i = 0; i < this.numFields(); i++) {
                if (!getFieldType(i).equals(o_td.getFieldType(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < this.numFields(); i++) {
            s += getFieldType(i) + "(" + getFieldName(i) + ")";
            if (i < this.numFields()  - 1) {
                s += ", ";
            }
        }
        return s;
    }
}
