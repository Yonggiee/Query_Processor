package qp.operators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Sort;
import qp.utils.Tuple;

/**
 * SortMergeJoin operator will merge two sorted relations given a condition list
 */
public class SortMergeJoin extends Join{
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

    private ArrayList<Tuple> rightSideTuples = null;
    private ArrayList<Tuple> leftSideTuples = null;
    private int leftItr;
    private int rightItr;

    private ArrayList<Tuple> bufferedTuples;
    private int bufferedItr;
    private boolean bufferedNoLongerMatched = true;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        numBuff = jn.getNumBuff();
        jointype = jn.getJoinType();
    }

    /**
     * Does the setup operations which include sorting the left and right
     **/
    public boolean open() {
        if (!left.open() || !right.open()) {
            return false;
        }
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        bufferedTuples = new ArrayList<>();
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

    /**
     * Outputs batch with merged tuples which satisfy the conditions list
     * Also backtracks when necessary to ensure correctness for duplicate attribute values
     * Assume buffered tuples to be able to fit in one buffer for simplicity.
     **/
    public Batch next() {
        Batch outbatch = new Batch(batchsize);
        int added = 0;
        int tuplesPerSide = batchsize * (numBuff - 1) / 2;

        if (rightSideTuples == null) {
            rightSideTuples = fillTuple(tuplesPerSide, rightSortedFile, 2);
            rightItr = 0;
        }
        if (leftSideTuples == null) {
            leftSideTuples = fillTuple(tuplesPerSide, leftSortedFile, 1);
            leftItr = 0;
        }
       
        while (rightSideTuples.size() > 0 && leftSideTuples.size() > 0) {
            Tuple left = leftSideTuples.get(leftItr);
            if (!bufferedNoLongerMatched) {
                checkCanAddBufferedTuples(left, outbatch);
                if (outbatch.size() == batchsize) {
                    break;
                }
            }
            Tuple right = rightSideTuples.get(rightItr);
            boolean canMerge = left.checkJoin(right, leftindex, rightindex);

            if (canMerge) {
                Tuple mergeTuple = left.joinWith(right);
                outbatch.add(mergeTuple);
                added += 1;
            }

            if (checkLeftIncrement(left, right) >= 0) {
                rightItr += 1;
                bufferedTuples.add(right);
                if (rightItr == rightSideTuples.size()) {
                    if (!rightSortedFileEndReached) {
                        rightSideTuples = fillTuple(tuplesPerSide, rightSortedFile, 2);
                        rightItr = 0;
                    } else {
                        rightSideTuples.clear();
                        break;
                    }
                }
            } else {
                leftItr += 1;
                bufferedItr = bufferedTuples.size() - 1;
                if (bufferedItr >= 0) {
                    bufferedNoLongerMatched = false;
                }
                if (leftItr == leftSideTuples.size()) {
                    if (!leftSortedFileEndReached) {
                        leftSideTuples = fillTuple(tuplesPerSide, leftSortedFile, 1);
                        leftItr = 0;
                    } else {
                        leftSideTuples.clear();
                        break;
                    }
                }
            }

            if (added == batchsize) {
                break;
            }
        }
        if (added == 0) {
            return null;
        }
        return outbatch;
    }

    /**
     * Checks if the left tuple has a higher value than the right tuple
     * according to a list of attributes
     **/
    private int checkLeftIncrement(Tuple left, Tuple right) {
        return Tuple.compareTuples(left, right, leftindex, rightindex);
    }

    /**
     * Add into batch buffered tuples by backtracking
     * Assume buffered tuple list to be able to fit in one buffer for simplicity
     **/
    private void checkCanAddBufferedTuples(Tuple toAdd, Batch batch) {
        while (batch.size() < batchsize && !bufferedNoLongerMatched) {
            Tuple currentBuffered = bufferedTuples.get(bufferedItr);
            boolean canMerge = toAdd.checkJoin(currentBuffered, leftindex, rightindex);
            if (canMerge) {
                Tuple mergeTuple = toAdd.joinWith(currentBuffered);
                batch.add(mergeTuple);
                bufferedItr -= 1;
                if (bufferedItr < 0) {
                    bufferedNoLongerMatched = true;
                }
            } else {
                bufferedNoLongerMatched = true;
            }
        }
    }

    /**
     * Simulate filling buffers with tuples from sorted object stream.
     * Set end of file reached if stream returns a null object.
     **/
    private ArrayList<Tuple> fillTuple(int totalTupleSize, ObjectInputStream sortedFile, int type) {
        ArrayList<Tuple> tuples = new ArrayList<>();

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
            } else {
                Tuple tuple = (Tuple) inStream;
                tuples.add(tuple);
            }
        }
        return tuples;
    }
    
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }
}
