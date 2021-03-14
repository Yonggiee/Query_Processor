package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.io.*;
import java.util.*;

public class GroupBy extends Operator {
    Operator base;
    int batchSize; 
    Batch outbatch;
    private ObjectInputStream lastSortedFile = null;
    private Sort sort;
    private boolean endOfSortedFile = false;
    private int numBuff;
    //
    private ArrayList attributes;
    private ArrayList<Integer> attributesIndex = new ArrayList<>();
    private Tuple prevtuple = null;
    //

    public GroupBy(Operator base, ArrayList attrs) {
        super(OpType.GROUPBY);
        this.base = base;
        this.attributes = attrs;
    }

    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int tuplesize = base.schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;
        this.sort = new Sort(base, numBuff, batchSize, true);
        lastSortedFile = this.sort.performSort();
        //
        for (int i = 0; i < attrs.size(); i++) {
            Attribute attribute = (Attribute) attributes.get(i);
            attributesIndex.add(schema.indexOf(attribute));
        }
        //
        return true;
    }

    public Batch next() {
        if (endOfSortedFile) {
            return null;
        }
        Batch outbatch = new Batch(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Object inStream;
            try {
                inStream = lastSortedFile.readObject();
                if (inStream == null) {
                    endOfSortedFile = true;
                    break;
                }
                Tuple outtuple = (Tuple) inStream;
                if (prevtuple == null || !isSameTuple(prevtuple, outtuple)){
                    outbatch.add(outtuple);
                    prevtuple = outtuple;
                }
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
        return outbatch;
    }

    private boolean isSameTuple(Tuple tuple1, Tuple tuple2){
        for (int i=0; i < attributesIndex.size(); i++) {
            if (Tuple.compareTuples(tuple1, tuple2, attributesIndex.get(i)) != 0) {
                return false;
            }
        }
        return true; 
    }
    
    public boolean close() {
        return super.close();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base; 
    }

    //
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newAttrs = new ArrayList<>();
        for (int i = 0; i < attrs.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) attributes.get(i)).clone();
            newAttrs.add(attribute);
        }
        GroupBy newGroupBy = new GroupBy(newBase, newAttrs);
        newGroupBy.setSchema(newBase.getSchema());
        return newGroupBy;
    }
    //
}