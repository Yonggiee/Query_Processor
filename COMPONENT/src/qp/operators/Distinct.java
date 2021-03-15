package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Sort;
import qp.utils.Tuple;

import java.io.*;

public class Distinct extends Operator {

    Operator base;
    int batchsize; // Number of tuples per out batch
    Batch outbatch; // Buffer page for output
    private ObjectInputStream lastSortedFile = null;
    private Sort sort;
    private boolean endOfSortedFile = false;
    private int numBuff;

    public Distinct(Operator base, int type, int numBuff) {
        super(type);
        this.base = base;
        this.numBuff = numBuff;
    }

    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int tuplesize = base.schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        this.sort = new Sort(base, numBuff, batchsize, true);
        lastSortedFile = this.sort.performSort();

        return true;
    }

    public Batch next() {
        if (endOfSortedFile) {
            return null;
        }
        Batch outbatch = new Batch(batchsize);
        int added = 0;

        for (int i = 0; i < batchsize; i++) {
            Object inStream;
            try {
                inStream = lastSortedFile.readObject();
                if (inStream == null) {
                    endOfSortedFile = true;
                    break;
                }
                Tuple outtuple = (Tuple) inStream;
                outbatch.add(outtuple);
                added += 1;
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
        if (added == 0) {
            return null;
        }
        return outbatch;
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

    public Object clone() {
        Operator newBase = (Operator) base.clone();
        Distinct newdist = new Distinct(newBase, numBuff, optype);
        newdist.setSchema((Schema) schema.clone());
        return newdist;
    }

}