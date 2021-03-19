package qp.operators;

import java.util.ArrayList;
import qp.utils.*;

public class Aggregate extends Operator {
    Operator base;                                          //Operator that holds this Aggregate Operator
    private ArrayList<AggregateAttribute> aggregateAttr;    //list of all aggregate attributes within this query
    ArrayList<Attribute> attrset;                           //list of all attributes within this query
    int[] attrIndex;                                        //array containing indexes of attributes within schema
    int batchsize;                                          //size of Batch object
    int tuplesize;                                          //size of Tuple object
    ArrayList<Tuple> outtuples;                             //Holds the tuples to be written out
    private boolean endOfStream;                            //Designates if there are any more batches to read in

    /**
     * Constructor for the Aggregate operator.
     * @param base Base operator instance that invokes this Aggregate operator
     * @param aggregate List of aggregate attributes within query
     * @param attrset List of all attributes within query
     * @param attrIndex Contains indexes of attributes within schema
     * @param tuplesize Size of a tuple.
     */
    public Aggregate(Operator base, ArrayList<AggregateAttribute> aggregate, ArrayList<Attribute> attrset, int[] attrIndex, int tuplesize) {
        super(OpType.AGGREGATE);
        this.base = base;
        this.aggregateAttr = aggregate;
        this.attrset = attrset;
        this.attrIndex = attrIndex;
        this.outtuples = new ArrayList<Tuple>();
        this.endOfStream = false;
        this.tuplesize = tuplesize;

    }

    /**
     * Opens up a stream to read in tuples in batches, and compute aggregate values for aggregate attributes
     */
    public boolean open() {
        batchsize = Batch.getPageSize() / tuplesize;
        while (!endOfStream) {
            Batch inbatch = base.next();
            if (inbatch == null) {
                endOfStream = true;
                continue;
            }
            for (int i = 0; i < inbatch.size(); i++) {
                if (i > inbatch.size()) {
                    endOfStream = true;
                    break;
                }
                Tuple currtuple = inbatch.get(i);
                outtuples.add(currtuple);
                for (int j = 0; j < aggregateAttr.size(); j++) {
                    aggregateAttr.get(j).setAggregateVal(currtuple);
                }
            }
        }
        return true;
    }

    /**
     * Reads in batch of tuples, outputs batch of tuples with attributes that require aggregation
     * @return A Batch containing the resultant tuples written out (including aggregate attributes)
     */
    public Batch next() {
        Batch outbatch = new Batch(batchsize);
        if (outtuples.size() == 0) {
            return null;
        }

        for (int i = 1; i <= batchsize; i++) {
            ArrayList<Object> updatedtuple = new ArrayList<>();
            if (outtuples.size() > 0) {
                Tuple originaltuple = outtuples.remove(0);
                updatedtuple.addAll(originaltuple.data());
                for (int j = 0; j < attrset.size(); j++) {
                    if (attrset.get(j).getAggType() == Attribute.NONE) {
                        continue;
                    } else {
                        for (AggregateAttribute aAttr : aggregateAttr) {
                            if (aAttr.getAttrIndex() == attrIndex[j] && aAttr.getAggregateType() == attrset.get(j).getAggType()) {
                                updatedtuple.add(aAttr.getAggregateVal());
                            }
                        }
                    }
                }
            } else {
                break;
            }
            Tuple outtuple = new Tuple(updatedtuple);
            outbatch.add(outtuple);
            // AA: added +1 " + i + " " + batchsize + " " + outtuples.size() + " " + outbatch.size());
        }
        if (outbatch.size() == 0) {
            return null;
        } else {
            return outbatch;
        }
    }

    public boolean close() {
        return super.close();
    }

    public Object clone() {
        Operator clone = (Operator) base.clone();
        Aggregate cloneAgg = new Aggregate(clone, aggregateAttr, attrset, attrIndex, tuplesize);
        cloneAgg.setSchema((Schema) schema.clone());
        return cloneAgg;
    }
}