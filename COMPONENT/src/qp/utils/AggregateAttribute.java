package qp.utils;

/**
 * Class for aggregate attributes.
 * Contains the different aggregation operation types (MIN, MAX, COUNT, AVG) as well as the associated operation for each aggregation type and data object type 
 * <b>Note: for this project we only look at Integer and String objects.</b>
 */
public class AggregateAttribute {

    private int attrIndex;              //index of aggregated column in data
    private int aggregateType;          //the type of aggregate operation for this aggregated attribute (MIN, MAX, COUNT, AVG)
    private Object aggregateVal;        //the value of the aggregated data
    private int totalSum;               //used for calculating AVG (ie the numerator)
    private int totalCount;             //used for calculating AVG (ie the denominator)
    private int aggregateValDataType;   //the data type of the column data to be aggregated (either Integer or String)

    /**
     * Construct the aggregate attribute and initialise the aggregateVal value according to the relevant aggregate operation
     */
    public AggregateAttribute(int attrIndex, int aggregateType) {
        this.attrIndex = attrIndex;
        this.aggregateType = aggregateType;
        this.aggregateValDataType = 0;
        switch (aggregateType) {
            case Attribute.MIN:
                aggregateVal = null;
            break;
            case Attribute.MAX:
                aggregateVal = null;
            break;
            case Attribute.COUNT:
                aggregateVal = 0;
            break;
            case Attribute.AVG:
                aggregateVal = 0;
                totalSum = 0;
                totalCount = 0;
            break;
        }
    }

    /**
     * Obtain the aggregate value for this attribute, according to operation type and data type
     */
    public void setAggregateVal(Tuple intuple){
        Object val = intuple.dataAt(attrIndex);
        if (val instanceof Integer) {
            aggregateValDataType = 1;
        } else if (val instanceof String) {
            aggregateValDataType = 2;
        } else {
            return;
        }

        /* if attribute is Integer */
        if (aggregateValDataType == 1) {
            switch (aggregateType) {
                case Attribute.MIN:
                    if (aggregateVal == null || (int) val < (int) aggregateVal){
                        aggregateVal = val;
                    }
                break;
                case Attribute.MAX:
                    if (aggregateVal == null || (int) aggregateVal < (int) val) {
                        aggregateVal = val;
                    }
                break;
                case Attribute.COUNT: 
                    aggregateVal = (int) aggregateVal + 1; 
                break;
                case Attribute.AVG:
                    totalSum = totalSum + (int)val;
                    totalCount += 1;
                    aggregateVal = totalSum / totalCount;
                break;
            }
        }

        /* if attribute is String */
        if (aggregateValDataType == 2) {
            String valString = (String) val;
            switch (aggregateType) {
                case Attribute.MIN:
                    if (aggregateVal==null || valString.compareTo((String) aggregateVal) > 0){
                        aggregateVal = valString;
                    }
                break;
                case Attribute.MAX:
                    if (aggregateVal == null || (valString.compareTo((String) aggregateVal) < 0)) {
                        aggregateVal = valString;
                    }
                break;
                case Attribute.COUNT: 
                    aggregateVal = (int) aggregateVal + 1; 
                break;
            }
        }
    }

    public Object getAggregateVal() {
        return aggregateVal;
    }

    public int getAttrIndex(){
        return attrIndex;
    }

    public int getAggregateType(){
        return aggregateType;
    }
    
}
