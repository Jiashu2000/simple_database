package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    int groupby_index;
    Field groupby_field;
    Type groupby_fieldtype;
    int agg_index;
    Op agg_op;
    TupleDesc td; 
    HashMap<Field, Integer> agg_res;
    HashMap<Field, Integer[]> agg_avg_res;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupby_index = gbfield;
        this.groupby_fieldtype = gbfieldtype;
        this.agg_index = afield;
        this.agg_op = what;
        this.agg_res = new HashMap<>();
        this.agg_avg_res = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field to_agg = tup.getField(this.agg_index);
        int to_agg_val = ((IntField) to_agg).getValue();
        this.groupby_field = null;
        if (this.groupby_index != Aggregator.NO_GROUPING) {
            this.groupby_field = tup.getField(this.groupby_index);
        }

        if (this.agg_op == Op.AVG) {
            if (this.agg_avg_res.containsKey(this.groupby_field)) {
                Integer[] count_sum = this.agg_avg_res.get(this.groupby_field);
                int count = count_sum[0];
                int sum = count_sum[1];
                this.agg_avg_res.put(this.groupby_field, new Integer[]{count+1, sum+to_agg_val});
            } else {
                this.agg_avg_res.put(this.groupby_field, new Integer[]{1, to_agg_val});
            }
            Integer[] count_sum = this.agg_avg_res.get(this.groupby_field);
            int count = count_sum[0];
            int sum = count_sum[1];
            this.agg_res.put(this.groupby_field, sum/count);
        } else if (this.agg_res.containsKey(this.groupby_field)) {
                Integer cur_agg = this.agg_res.get(this.groupby_field);
                Integer new_agg = update_agg(cur_agg, to_agg_val);
                this.agg_res.put(this.groupby_field, new_agg);
        } else if (this.agg_op == Op.COUNT) {
            this.agg_res.put(this.groupby_field, 1);
        } else {
            this.agg_res.put(this.groupby_field, to_agg_val);
        }
    }

    private Integer update_agg(Integer cur_agg, int to_agg_val) {
        switch (this.agg_op) {
            case COUNT:
                return cur_agg + 1;
            case MAX:
                return Math.max(cur_agg, to_agg_val);
            case MIN:
                return Math.min(cur_agg, to_agg_val);
            case SUM:
                return cur_agg + to_agg_val;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> agg_tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : this.agg_res.entrySet()) {
            if (this.groupby_index == Aggregator.NO_GROUPING) {
                this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(entry.getValue()));
                agg_tuples.add(t);
            } else {
                this.td = new TupleDesc(new Type[]{this.groupby_fieldtype, Type.INT_TYPE});
                Tuple t = new Tuple(this.td);
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
                agg_tuples.add(t);
            }
        }
        return new TupleIterator(this.td, agg_tuples);   
    }

}
