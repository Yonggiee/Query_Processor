package qp.operators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;
import qp.utils.Sort;
import qp.utils.Tuple;

public class SortMergeJoin extends Join{
    private Schema schema;
    private int numBuff;
    private int batchsize;
	private ArrayList<Integer> leftindex;
    private ArrayList<Integer> rightindex;
    private List<OrderType> leftOrderType;
    private List<OrderType> rightOrderType;

    private Sort leftSort;
    private ObjectInputStream leftSortedFile = null;
    private boolean leftSortedFileEndReached = false;

    private Sort rightSort; 
    private ObjectInputStream rightSortedFile = null;
    private boolean rightSortedFileEndReached = false;

    private ArrayList<Tuple> rightSideTuples;
    private ArrayList<Tuple> leftSideTuples;
    private int leftItr;
    private int rightItr;

    private ArrayList<Tuple> bufferedTuples;
    private int bufferedItr;
    private boolean bufferedNoLongerMatched = false;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        numBuff = jn.getNumBuff();
        jointype = jn.getJoinType();
        System.out.println("LALALALALA" + schema);
    }

    /**
     * During open finds the index of the join attributes * Materializes the right
     * hand side into a file * Opens the connections
     **/
    public boolean open() {
        if (!left.open() || !right.open()) {
            return false;
        }
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        bufferedTuples = new ArrayList<>();
        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();

        leftOrderType = new ArrayList<OrderType>();
        rightOrderType = new ArrayList<OrderType>();

        conditionList = Condition.sortConditionList(conditionList);

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            OrderType leftOrder = new OrderType(leftattr, OrderType.Order.ASC);
            leftOrderType.add(leftOrder);

            Attribute rightattr = (Attribute) con.getRhs();
            rightindex.add(right.getSchema().indexOf(rightattr));
            OrderType rightOrder = new OrderType(rightattr, OrderType.Order.ASC);
            rightOrderType.add(rightOrder);
        }

        leftSort = new Sort(left, numBuff, leftOrderType, batchsize);
        leftSortedFile = this.leftSort.performSort();
        rightSort = new Sort(right, numBuff, rightOrderType, batchsize);
        rightSortedFile = this.rightSort.performSort();

        return true;
    }

    public Batch next() {
        Batch outbatch = new Batch(batchsize);
        int added = 0;
        int tuplesPerSide = batchsize * (numBuff - 1) / 2;

        if (rightSideTuples == null) {
            rightSideTuples = fillTuple(tuplesPerSide, rightSortedFile, 2);
            leftItr = 0;
        }
        if (leftSideTuples == null) {
            leftSideTuples = fillTuple(tuplesPerSide, leftSortedFile, 1);
            rightItr = 0;
        }
       
        while (rightSideTuples.size() != 0 && leftSideTuples.size() != 0) {
            boolean canMerge = true;
            Tuple left = leftSideTuples.get(leftItr);
            if (!bufferedNoLongerMatched) {
                checkCanAddBufferedTuples(left, outbatch);
                if (outbatch.size() == batchsize) {
                    break;
                }
            }
            Tuple right = rightSideTuples.get(rightItr);

            for (int i = 0; i < leftindex.size(); i++) {
                canMerge = canMerge && left.checkJoin(right, leftindex.get(i), rightindex.get(i));
            }

            if (canMerge) {
                Tuple mergeTuple = left.joinWith(right);
                outbatch.add(mergeTuple);
                bufferedTuples.add(right);
                bufferedItr = bufferedTuples.size() - 1;
                bufferedNoLongerMatched = false;

                added += 1;
                rightItr += 1;
                if (rightItr == rightSideTuples.size()) {
                    if (!rightSortedFileEndReached) {
                        rightSideTuples = fillTuple(tuplesPerSide, rightSortedFile, 2);
                        rightItr = 0;
                    } else {
                        break;
                    }
                }
                if (added == batchsize) {
                    break;
                }
            } else {
                leftItr += 1;
                if (leftItr == leftSideTuples.size()) {
                    if (!leftSortedFileEndReached) {
                        leftSideTuples = fillTuple(tuplesPerSide, leftSortedFile, 1);
                        leftItr = 0;
                    } else {
                        break;
                    }
                }
            }
        }
        return outbatch;
    }

    private void checkCanAddBufferedTuples(Tuple toAdd, Batch batch) {
        while (batch.size() < batchsize && !bufferedNoLongerMatched) {
            Tuple currentBuffered = bufferedTuples.get(bufferedItr);

            boolean canMerge = true;
            for (int i = 0; i < leftindex.size(); i++) {
                canMerge = canMerge && toAdd.checkJoin(currentBuffered, leftindex.get(i), rightindex.get(i));
            }
            if (canMerge) {
                Tuple mergeTuple = toAdd.joinWith(currentBuffered);
                batch.add(mergeTuple);
                bufferedItr -= 1;
                if (bufferedItr == 0) {
                    bufferedNoLongerMatched = true;
                }
            } else {
                bufferedNoLongerMatched = true;
            }
        }
    }

    private ArrayList<Tuple> fillTuple(int totalTupleSize, ObjectInputStream sortedFile, int type) {
        ArrayList<Tuple> rightTuples = new ArrayList<>();

        for (int i = 0; i < totalTupleSize; i++) {
            Object inStream = null;
            try {
                inStream = sortedFile.readObject();
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
            if (inStream == null) {
                if (type == 1) {
                    leftSortedFileEndReached = true;
                } else if (type == 2) {
                    rightSortedFileEndReached = true;
                }
                break;
            }
        }

        return rightTuples;
    }
    
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }
}