package qp.utils;

public class AggregateAttribute {

    private int attrIndex;
    private int aggregateType;
    private Object aggregateVal;
    private int sum;
    private int count;
    private int aggregateValDataType;


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
                sum = 0;
                count = 0;
                break;
        }
    }

    public void setAggregateVal(Tuple intuple){
        Object val = intuple.dataAt(attrIndex);
        if (val instanceof Integer) {
            aggregateValDataType = 1;
        } else if (val instanceof String) {
            aggregateValDataType = 2;
        } else {
            return;
        }

        if (aggregateValDataType == 1) {
            if (aggregateType == Attribute.MIN) {
                if (aggregateVal == null || (int) val < (int) aggregateVal){
                    aggregateVal = val;
                }
            } else if (aggregateType == Attribute.MAX) {
                if (aggregateVal == null || (int) aggregateVal < (int) val) {
                    aggregateVal = val;
                }
            } else if (aggregateType == Attribute.COUNT) {
                aggregateVal = (int)aggregateVal + 1;
            } else if (aggregateType == Attribute.AVG) {
                sum = sum + (int)val;
                count += 1;
                aggregateVal= sum / count;
            }
        }
        if (aggregateValDataType == 2) {
            String valString = (String) val;
            if (aggregateType == Attribute.MIN) {
                if (aggregateVal==null || valString.compareTo((String) aggregateVal) > 0){
                    aggregateVal = valString;
                }
            } else if(aggregateType == Attribute.MAX) {
                if (aggregateVal == null || (valString.compareTo((String) aggregateVal) < 0)) {
                    aggregateVal = valString;
                }
            } else if (aggregateType == Attribute.COUNT) {
                aggregateVal = (int) aggregateVal + 1;
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
