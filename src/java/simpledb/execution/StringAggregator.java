package simpledb.execution;

import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    protected HashMap<Field, Integer> fieldMap;
    private int count = 0;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        if (what == Op.COUNT) {
            this.gbfield = gbfield;
            this.gbfieldtype = gbfieldtype;
            this.afield = afield;
            this.what = what;
            this.fieldMap = new HashMap<>();
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield ==Aggregator.NO_GROUPING) {
            count +=1;
        } else {
            Field currentField = (Field) tup.getField(gbfield);
            if (! fieldMap.containsKey(currentField)) {
                fieldMap.put(currentField, 0);
            }
            int currentValue = fieldMap.get(currentField);
            fieldMap.put(currentField, currentValue + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private TupleDesc tupleDesc;
            private Tuple[] aggregateVal;
            private int current_index = 0;

            @Override
            public void open() throws DbException, TransactionAbortedException{
                if (gbfield == Aggregator.NO_GROUPING) {
                    // if no grouping
                    aggregateVal = new Tuple[1];
                    tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(count));
                    aggregateVal[0] = tuple;
                } else {
                    // if there's grouping
                    aggregateVal = new Tuple[fieldMap.size()];
                    tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                    int i = 0;
                    for (Field gbField: fieldMap.keySet()) {
                        Tuple tuple = new Tuple(tupleDesc);
                        tuple.setField(0, gbField);
                        tuple.setField(1, new IntField(fieldMap.get(gbField)));
                        aggregateVal[i] = tuple;
                        i++;
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException{
                if (current_index < aggregateVal.length) {
                    return true;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.hasNext()) {
                    Tuple nextTuple = aggregateVal[current_index++];
                    return nextTuple;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                current_index = 0;
            }

            @Override
            public void close(){
                tupleDesc = null;
                aggregateVal = null;
                current_index = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }
            
        };
    }
}
