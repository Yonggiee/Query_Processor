package qp.operators;

import java.util.ArrayList;
import qp.utils.*;

public class Aggregate extends Operator {
    Operator base;
    private ArrayList<AggregateAttribute> aggregateAttr;
    ArrayList<Attribute> attrset;
    int[] attrIndex;
    int batchsize;
    int tuplesize;
    ArrayList<Tuple> outtuples;
    private boolean endOfStream;


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