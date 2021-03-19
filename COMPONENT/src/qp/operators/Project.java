/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.AggregateAttribute;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    ArrayList<AggregateAttribute> aggregateAttrs = new ArrayList<AggregateAttribute>(); //Set of attributes to perform aggregation on
    Operator aggOperator;
    boolean aggregateIsPresent;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        if (batchsize == 0) {
            System.out.println(
                    "Terminating as page size too small for one tuple... At least " + tuplesize + " is required.");
            return false;
        }

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        aggregateIsPresent = false;

        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;

            if (attr.getAggType() != Attribute.NONE) {
                aggregateIsPresent = true;
                AggregateAttribute newAggIndex = new AggregateAttribute(index, attr.getAggType());
                aggregateAttrs.add(newAggIndex);
            }
        }

        if(aggregateIsPresent) {
            aggOperator = new Aggregate(base, aggregateAttrs, attrset, attrIndex, tuplesize);
            aggOperator.open();
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        Tuple prevtuple = null;

        if(aggregateIsPresent) {
            inbatch = aggOperator.next();
        } else {
            inbatch = base.next();
        }

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            boolean hasAggregate = false;
            ArrayList<Object> updatedtuple = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                if (attrset.get(j).getAggType() == Attribute.NONE) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    updatedtuple.add(data);
                } else {
                    hasAggregate = true;
                    int count = 0;
                    for (AggregateAttribute aAttr : aggregateAttrs) {
                        count += 1;
                        if (aAttr.getAttrIndex() == attrIndex[j] && aAttr.getAggregateType() == attrset.get(j).getAggType()) {
                            Object data = basetuple.dataAt(base.getSchema().getNumCols()+count-1);
                            updatedtuple.add(data);
                            break;
                        }
                    }
                }
            }
            
            Tuple outtuple = new Tuple(updatedtuple);
            if(hasAggregate && prevtuple == null){
                outbatch.add(outtuple);
                prevtuple = outtuple;

            } else if(!hasAggregate) {
                outbatch.add(outtuple);
                prevtuple = outtuple;
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
/*
    private boolean checkDuplicate(Tuple previoustuple, Tuple currenttuple) {
        //Compare every attribute of the tuples to check for duplicate
        for(int i=0; i<currenttuple.data().size(); i++){
            if(Tuple.compareTuples(previoustuple, currenttuple, i)!=0) {
                return true;
            }
        }
        return false;
    }
    */
}
