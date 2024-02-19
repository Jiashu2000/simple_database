package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private int agg_index;
    private int groupby_index;
    private Aggregator.Op agg_op;
    private DbIterator child;
    private TupleDesc child_td;
    private Type groupby_field_type;
    private Type agg_field_type;
    private Aggregator aggregator;
    private DbIterator aggregate_iterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.agg_index = afield;
        this.groupby_index = gfield;
        this.agg_op = aop;
        this.child_td = child.getTupleDesc();
        this.groupby_field_type = gfield == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gfield);
        this.agg_field_type = this.child_td.getFieldType(afield);
        if (this.agg_field_type == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(this.groupby_index, this.groupby_field_type, this.agg_index, this.agg_op);
        } else if (this.agg_field_type == Type.STRING_TYPE) {
            aggregator = new StringAggregator(this.groupby_index, this.groupby_field_type, this.agg_index, this.agg_op);
        }
        this.aggregate_iterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
	    return this.groupby_index;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // some code goes here
        if (this.groupby_index == Aggregator.NO_GROUPING) return null;
	    return this.aggregate_iterator.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
	    return this.agg_index;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    // some code goes here
        if (this.groupby_index == Aggregator.NO_GROUPING) {
            return this.aggregate_iterator.getTupleDesc().getFieldName(0);
        }
	    return  this.aggregate_iterator.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
	    return this.agg_op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        this.child.open();
        super.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.aggregate_iterator = this.aggregator.iterator();
        this.aggregate_iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggregate_iterator.hasNext()) {
            return this.aggregate_iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggregate_iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        Type[] types;
        String[] names;
        String agg_name = this.child_td.getFieldName(this.agg_index);
        if (this.groupby_index == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{agg_name};
        } else {
            types = new Type[]{this.groupby_field_type, Type.INT_TYPE}; 
            names = new String[]{this.child_td.getFieldName(this.groupby_index), agg_name};
        }
        TupleDesc td = new TupleDesc(types, names);
        return td;
    }

    public void close() {
	    // some code goes here
        super.close();
        this.aggregate_iterator.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    // some code goes here
	    return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    // some code goes here
        this.child = children[0];
    }
    
}
