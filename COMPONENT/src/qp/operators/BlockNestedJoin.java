/**
 * Page Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    ArrayList<Integer> exprindex;  // Join conditions of the join attributes in both tables
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    List<Batch> leftblock = new LinkedList<>();          // Left Block that contains M-2 buffer pages
    ArrayList<Tuple> leftTuples = new ArrayList<>();    // Flattened list of tuples (entire left block)
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * Private function for this class to attempt to (re-)populate the block.
     * Returns true when there is > 0 populated, and false when there is nothing to populate.
     */
    private void populateBlock() {
        this.leftblock.clear();
        this.leftTuples.clear();

        for (int i = 0; i < numBuff-2; i++) {
            Batch currentBatch = (Batch) left.next();
            if (currentBatch == null) {
                break;
            }
            this.leftblock.add(currentBatch);
        }
        // Flatten tuples across pages into left block
        for (Batch b: this.leftblock) {
            for (int i = 0; i < b.size(); i++) {
                this.leftTuples.add(b.get(i));
            }
        }
        if (!this.leftblock.isEmpty()) {
            /** Whenever a new left page came, we have to start the
            ** scanning of right table
            **/
            try {
                in = new ObjectInputStream(new FileInputStream(rfname));
                eosr = false;
            } catch (IOException io) {
                System.err.println("NestedJoin:error in reading the file");
                System.exit(1);
            }
        }
    }
    
    /** select number of tuples per batch **/
    private boolean setBatchSize() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        this.leftblock = new LinkedList<>();
        if (batchsize == 0) {
            System.out.println(
                    "Terminating as page size too small for one tuple... At least " + tuplesize + " is required.");
            return false;
        }
        return true;
    }

    /** find indices attributes of join conditions **/
    private void setIndexAttribute() {
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        exprindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            int compareType = con.getExprType();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
            exprindex.add(compareType);
        }
    }
    
    /** initialize the cursors of input buffers **/
    private void resetCursors() {
        this.lcurs = 0;
        this.rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;
    }

    /** Right hand side table is to be materialized
     ** for the Nested join to perform
    **/
    private boolean materializeRight() {
        Batch rightpage;        
        
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            return right.close();
        }
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        boolean can = setBatchSize();
        if (!can) {
            return false;
        }
        setIndexAttribute();
        resetCursors();
        return materializeRight() && left.open();
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;
        if (eosl && this.leftblock.isEmpty()) {
            this.close();
            return null;
        }

        outbatch = new Batch(batchsize);
        
        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr) {
                this.populateBlock();
                if (this.leftblock.isEmpty()) {
                    eosl = true;
                    return outbatch;
                }
            }

            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (i = lcurs; i < leftTuples.size(); i++) {
                        for (j = rcurs; j < rightbatch.size(); j++) {
                            Tuple lefttuple = leftTuples.get(i);
                            Tuple righttuple = rightbatch.get(j);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex, exprindex)) {
                            // if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    if (i == leftTuples.size() - 1 && j == rightbatch.size() - 1) {         //case 1: left & right pages complete
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftTuples.size() - 1 && j == rightbatch.size() - 1) {  //case 2: only right complete
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else if (i == leftTuples.size() - 1 && j != rightbatch.size() - 1) {  //case 3: only left complete
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {                                                                //case 4: both sides not complete
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
