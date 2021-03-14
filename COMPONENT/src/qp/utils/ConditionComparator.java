package qp.utils;

import java.util.Comparator;

public class ConditionComparator implements Comparator<Condition> {

    @Override
    public int compare(Condition condition1, Condition condition2) {
        if (condition1.exprtype == 5) {
            return -1;
        } else if (condition1.exprtype == 5) {
            return -1;
        } else {
            return condition1.exprtype - condition2.exprtype;
        }
    }
}
