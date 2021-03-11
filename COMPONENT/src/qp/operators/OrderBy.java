package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Sort;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

/**
 * The OrderBy operator will order specified attributes and order type (ASC or
 * DESC). This is implemented using external sorting.
 */
public class OrderBy extends Operator {

    private Operator base;
    private List<OrderType> orderByTypeList;
    private int numBuff;

    private int batchsize;
    private ObjectInputStream lastSortedFile = null;
    private boolean endOfSortedFile = false;
    private Sort sort;

    public OrderBy(Operator base, List<OrderType> orderTypes, int numBuffers) {
        super(OpType.ORDERBY);
        this.base = base;
        this.orderByTypeList = orderTypes;
        this.numBuff = numBuffers;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getBuffers() {
        return numBuff;
    }

    /**
     * Opens the OrderBy operator and performs the necessary initialisation, and the
     * external sorting algorithm for the ordering by specified OrderTypes.
     * 
     * @return true if operator is successfully opened and executed.
     */
    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int tuplesize = base.schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        this.sort = new Sort(base, numBuff, orderByTypeList, batchsize);
        System.out.println("-------- OrderBy Operator open --------");
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


    /**
     * Close operator: refresh sorted runs, close stream.
     */
    public boolean close() {
        System.out.println("-------- OrderBy operator close --------");
        return super.close();
    }

    public Object clone() {
        Operator clone = (Operator) base.clone();
        OrderBy cloneOB = new OrderBy(clone, orderByTypeList, numBuff);
        cloneOB.setSchema((Schema) schema.clone());
        return cloneOB;
    }

    public int getPages() {
        return 0;
    }

}
