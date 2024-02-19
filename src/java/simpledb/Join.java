package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private TupleIterator return_tuple;

    // https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_join_buffer_size
    private final int join_buffer_size = 262144;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.child1.open();
        this.child2.open();
        super.open();
        // if (this.p.getOperator().equals(Predicate.Op.EQUALS)) {
        //     this.return_tuple = hashJoin();
        // }
        // this.return_tuple = sortMergeJoin();
        // this.return_tuple = nestedLoopJoin();
        this.return_tuple = blockNestedLoopJoin();
        this.return_tuple.open();
    }

    private Tuple joinTuples(Tuple t1, Tuple t2) {
        Tuple joined_tuple = new Tuple(getTupleDesc());
        int n1 = t1.getTupleDesc().numFields();
        int n2 = t2.getTupleDesc().numFields();
        for (int i = 0; i < n1; i++) {
            joined_tuple.setField(i, t1.getField(i));
        }
        for (int i = 0; i < n2; i++) {
            joined_tuple.setField(n1+i, t2.getField(i));
        }
        return joined_tuple;
    }

    private TupleIterator hashJoin() throws DbException, TransactionAbortedException {

        if (!this.p.getOperator().equals(Predicate.Op.EQUALS)) {
            throw new DbException("Hash Join only works for equal join");
        }

        // hash the smaller table
        ConcurrentHashMap<Field, ArrayList<Tuple>> child_hash = new ConcurrentHashMap<>();
        // how to compare the size of child1 and child2 to determine which one to hash?
        while (this.child1.hasNext()) {
            Tuple t = this.child1.next();
            Field t_f = t.getField(this.p.getField1());
            if (!child_hash.containsKey(t_f)) {
                child_hash.put(t_f, new ArrayList<>());
            }
            child_hash.get(t_f).add(t);
        }

        // probe the larger table
        LinkedList<Tuple> joined_tuples = new LinkedList<>();
        while (this.child2.hasNext()) {
            Tuple t = this.child2.next();
            Field t_f = t.getField(this.p.getField2());
            if (child_hash.containsKey(t_f)) {
                ArrayList<Tuple> matched_tuples = child_hash.get(t_f);
                for (Tuple m_t : matched_tuples) {
                    Tuple joined_tuple = joinTuples(m_t, t);
                    joined_tuples.add(joined_tuple);
                }
            }
        }
        return new TupleIterator(getTupleDesc(), joined_tuples);
    }

    private TupleIterator sortMergeJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> joined_tuples = new LinkedList<>();
        LinkedList<Tuple> l1 = new LinkedList<>();
        LinkedList<Tuple> l2 = new LinkedList<>();

        this.child1.rewind();
        while (this.child1.hasNext()) {
            Tuple t1 = this.child1.next();
            l1.add(t1);
        }
        this.child2.rewind();
        while (this.child2.hasNext()) {
            Tuple t2 = this.child2.next();
            l2.add(t2);
        }
        int field_index1 = this.p.getField1();
        int field_index2 = this.p.getField2();
        sort_helper(l1, field_index1);
        sort_helper(l2, field_index2);

        // equal join case
        if (this.p.getOperator().equals(Predicate.Op.EQUALS)) {
            int ptr1 = 0; 
            int ptr2 = 0;
            JoinPredicate eq = new JoinPredicate(field_index1, Predicate.Op.EQUALS, field_index2);
            JoinPredicate gt = new JoinPredicate(field_index1, Predicate.Op.GREATER_THAN, field_index2);
            while (ptr1 < l1.size() && ptr2 < l2.size()) {
                Tuple t1 = l1.get(ptr1);
                Tuple t2 = l2.get(ptr2);
                if (eq.filter(t1, t2)) {
                    int begin1 = ptr1;
                    int begin2 = ptr2;
                    while (ptr1 < l1.size() && eq.filter(l1.get(ptr1), t2)) ptr1++;
                    while (ptr2 < l2.size() && eq.filter(t1, l2.get(ptr2))) ptr2++;
                    int end1 = ptr1;
                    int end2 = ptr2;

                    for (int i = begin1; i < end1; i++) {
                        for (int j = begin2; j  < end2; j++) {
                            joined_tuples.add(joinTuples(l1.get(i), l2.get(j)));
                        }
                    }
                } else if (gt.filter(t1, t2)) {
                    ptr2++;
                } else {
                    ptr1++;
                }
            }
        } else if (this.p.getOperator().equals(Predicate.Op.GREATER_THAN_OR_EQ) || this.p.getOperator().equals(Predicate.Op.GREATER_THAN)) {
            int ptr1 = 0;
            int ptr2 = 0;
            while (ptr1 < l1.size()) {
                Tuple t1 = l1.get(ptr1);
                ptr2 = 0;
                while (ptr2 < l2.size()) {
                    Tuple t2 = l2.get(ptr2++);
                    if (this.p.filter(t1, t2)) {
                        joined_tuples.add(joinTuples(t1, t2));
                    } else {
                        break;
                    }
                }
                ptr1++;
            }
        } else if (this.p.getOperator().equals(Predicate.Op.LESS_THAN_OR_EQ) || this.p.getOperator().equals(Predicate.Op.LESS_THAN)) {
            int ptr1 = 0;
            int ptr2 = 0;
            while (ptr1 < l1.size()) {
                Tuple t1 = l1.get(ptr1);
                ptr2 = l2.size() - 1;
                while (ptr2 >= 0) {
                    Tuple t2 = l2.get(ptr2--);
                    if (this.p.filter(t1, t2)) {
                        joined_tuples.add(joinTuples(t1, t2));
                    } else {
                        break;
                    }
                }
                ptr1++;
            }         
        }

        return new TupleIterator(getTupleDesc(), joined_tuples);
    }


    private void sort_helper(List<Tuple> l, int field_index) {

        JoinPredicate lt = new JoinPredicate(field_index, Predicate.Op.LESS_THAN, field_index);
        JoinPredicate gt = new JoinPredicate(field_index, Predicate.Op.GREATER_THAN, field_index);

        Comparator<Tuple> comparator = new Comparator<Tuple>() {

            @Override
            public int compare(Tuple o1, Tuple o2) {
                if (lt.filter(o1, o2)) {
                    return -1;
                } else if (gt.filter(o1, o2)) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        Collections.sort(l, comparator);
    }

    private TupleIterator nestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> joined_tuples = new LinkedList<>();
        this.child1.rewind();
        while (this.child1.hasNext()) {
            Tuple t1 = this.child1.next();
            this.child2.rewind();
            while (this.child2.hasNext()) {
                Tuple t2 = this.child2.next();
                if (this.p.filter(t1, t2)) {
                    Tuple merged = joinTuples(t1, t2);
                    joined_tuples.add(merged);
                }
            }
        }
        return new TupleIterator(getTupleDesc(), joined_tuples);
    }

    private TupleIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> joined_tuples = new LinkedList<>();

        // create buffers to store tuples from child1, child2
        int buffer1_num_tuples = (int) Math.floor(this.join_buffer_size / this.child1.getTupleDesc().getSize());
        int buffer2_num_tuples = (int) Math.floor(this.join_buffer_size / this.child2.getTupleDesc().getSize());
        Tuple[] buffer1 = new Tuple[buffer1_num_tuples];
        Tuple[] buffer2 = new Tuple[buffer2_num_tuples];
        int b1_index = 0;
        int b2_index = 0;

        this.child1.rewind();
        while (this.child1.hasNext()) {
            Tuple t1 = this.child1.next();
            buffer1[b1_index] = t1;
            b1_index += 1;
            // once buffer1 is full, start to cache tuples from child2 to buffer2
            if (b1_index >= buffer1_num_tuples) {
                this.child2.rewind();
                while (child2.hasNext()) {
                    Tuple t2 = this.child2.next();
                    buffer2[b2_index++] = t2;
                    // once buffer2 is full, nested loop join two buffers
                    if (b2_index >= buffer2_num_tuples) {
                        bnlj_helper(joined_tuples, buffer1, buffer2);
                        // clean buffer2 for the next batch of tuples from child2
                        Arrays.fill(buffer2, null);
                        b2_index = 0;
                    }
                }
                // loop join remaining tuples in buffer2
                if (b2_index > 0 && b2_index < buffer2_num_tuples) {
                    bnlj_helper(joined_tuples, buffer1, buffer2);
                    Arrays.fill(buffer2, null);
                    b2_index = 0;
                }
                // clean buffer1 for the next batch of tuples from child1
                Arrays.fill(buffer1, null);
                b1_index = 0;
            }
        }
        // remaining tuples in buffer1
        if (b1_index > 0 && b1_index < buffer1_num_tuples) {
            this.child2.rewind();
            while (child2.hasNext()) {
                Tuple t2 = this.child2.next();
                buffer2[b2_index++] = t2;
                // once buffer2 is full, nested loop join two buffers
                if (b2_index >= buffer2_num_tuples) {
                    bnlj_helper(joined_tuples, buffer1, buffer2);
                    // clean buffer2 for the next batch of tuples from child2
                    Arrays.fill(buffer2, null);
                    b2_index = 0;
                }
            }
            // loop join remaining tuples in buffer2
            if (b2_index > 0 && b2_index < buffer2_num_tuples) {
                bnlj_helper(joined_tuples, buffer1, buffer2);
                Arrays.fill(buffer2, null);
                b2_index = 0;
            } 
        }
        return new TupleIterator(getTupleDesc(), joined_tuples);
    }

    private void bnlj_helper(LinkedList<Tuple> joined_tuples, Tuple[] buffer1, Tuple[] buffer2) {
        // buffer1 and buffer2 might not be full, include nulls.
        int b1_num = buffer1.length - 1;
        int b2_num = buffer2.length - 1;
        for (; b1_num> 0 && buffer1[b1_num] == null; b1_num--);
        for (; b2_num > 0 && buffer2[b2_num] == null; b2_num--);

        Tuple[] full_b1 = new Tuple[b1_num+1];
        Tuple[] full_b2 = new Tuple[b2_num+1];

        System.arraycopy(buffer1, 0, full_b1, 0, full_b1.length);
        System.arraycopy(buffer2, 0, full_b2, 0, full_b2.length);

        for (Tuple t1: full_b1) {
            for (Tuple t2: full_b2) {
                if (this.p.filter(t1, t2)) {
                    joined_tuples.add(joinTuples(t1, t2));
                }
            }
        }
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();
    }

    // Rewind the stream back to the start. 
    // This could be necessary for implementing e.g. nested loop joins.
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
        open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.return_tuple.hasNext()) {
            return this.return_tuple.next();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }
}
