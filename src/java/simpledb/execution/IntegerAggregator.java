package simpledb.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private final int gbfield;
    private final Type gbfieldtype;
    private int afield;
    private Op what;

    protected Map<Field, Integer> gbHash;
    protected Map<Field, int[]> gbAvgHash;
    private Field nonGbKey;

    private static final long serialVersionUID = 1L;

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
        //some code goes here
        this.gbfield = gbfield; //grouping of groupby field
        this.gbfieldtype = gbfieldtype; //type of groupby field
        this.afield = afield; //aggregate field number
        this.what = what; //sql opperand function
        this.gbHash = new HashMap<>(); //stores a hashmap of the field and its value
        this.gbAvgHash = new HashMap<>();
        this.nonGbKey = null;
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
        //first must check for grouping 
        Field curGbField;
        IntField curAgField;
        int curCountVal;
        int curSumVal;
        int curMaxVal;
        int curMinVal;

        if (gbfield ==Aggregator.NO_GROUPING) {
            curGbField = null;
        } else {
            curGbField = tup.getField(gbfield);
        }
        //get the column
        curAgField = (IntField) tup.getField(afield);
        //get value from column
        Integer curVal = curAgField.getValue();

        switch (this.what) {
            case MAX: //check if field exists in gbHash, if does not exist insert into the hash
                if (!gbHash.containsKey(curGbField)) {
                    gbHash.put(curGbField, curVal);
                } else { 
                    curMaxVal = gbHash.get(curGbField);
                    //compare curVal and curMaxVal, insert larger value into gbHash
                    gbHash.put(curGbField, Math.max(curMaxVal, curVal));
                }
                return;

            case SUM:
                if (!gbHash.containsKey(curGbField)) {
                    gbHash.put(curGbField, curVal);
                } else {
                    curSumVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, curSumVal + curVal);
                }
            return;

            case COUNT:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, 1);
                } else {
                    curCountVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, curCountVal +1);
                }
            return;

            case MIN:
                if (!gbHash.containsKey(curGbField)){
                    gbHash.put(curGbField, curVal);
                } else {
                    curMinVal = gbHash.get(curGbField);
                    gbHash.put(curGbField, Math.min(curMinVal, curVal));
                }
            return;

            case AVG:
                if (!gbAvgHash.containsKey(curGbField)){
                    gbAvgHash.put(curGbField, new int[]{curVal, 1});
                } else {
                    curSumVal = gbAvgHash.get(curGbField)[0];
                    curCountVal = gbAvgHash.get(curGbField)[1];
                    gbAvgHash.put(curGbField, new int[]{curSumVal + curVal, curCountVal + 1});
                }
            return;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
                    if (what == Op.AVG) {
                        // if AVG, access gbAvgHash
                        tuple.setField(0, new IntField(gbAvgHash.get(nonGbKey)[0] / gbAvgHash.get(nonGbKey)[1]));
                    } else {
                        // if not AVG, access gbHash
                        tuple.setField(0, new IntField(gbHash.get(nonGbKey)));
                    }
                    aggregateVal[0] = tuple;
                } else {
                    // if there's grouping
                    if (what == Op.AVG) {
                        aggregateVal = new Tuple[gbAvgHash.size()];
                        tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        int i = 0;
                        for (Field gbField: gbAvgHash.keySet()) {
                            Tuple tuple = new Tuple(tupleDesc);
                            tuple.setField(0, gbField);
                            tuple.setField(1, new IntField(gbAvgHash.get(gbField)[0] / gbAvgHash.get(gbField)[1]));
                            aggregateVal[i] = tuple;
                            i++;
                        }

                    } else {
                        aggregateVal = new Tuple[gbHash.size()];
                        tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        int i = 0;
                        for (Field gbField: gbHash.keySet()) {
                            Tuple tuple = new Tuple(tupleDesc);
                            tuple.setField(0, gbField);
                            tuple.setField(1, new IntField(gbHash.get(gbField)));
                            aggregateVal[i] = tuple;
                            i++;
                        }
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
                    return aggregateVal[current_index++];
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
