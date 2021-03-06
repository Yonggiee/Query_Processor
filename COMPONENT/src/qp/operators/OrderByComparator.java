package qp.operators;

import qp.operators.OrderType.Order;
import qp.utils.Attribute;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Comparator which sorts tuples according to the OrderTypes
 */
public class OrderByComparator implements Comparator<Tuple> {
    private Schema schema;
    private List<OrderType> orderTypeList;

    public OrderByComparator(Schema schema) {
        this.schema = schema;
        ArrayList<Attribute> attrList = schema.getAttList();
        orderTypeList = new ArrayList<>();
        for (int i = 0; i < attrList.size(); i++) {
            OrderType ot = new OrderType(attrList.get(i), Order.ASC);
            orderTypeList.add(ot);
        }
    }

    public OrderByComparator(Schema schema, List<OrderType> orderTypeList) {
        this.schema = schema;
        this.orderTypeList = orderTypeList;
    }

    @Override
    public int compare(Tuple tuple1, Tuple tuple2) {
        for (int i = 0; i < orderTypeList.size(); i++) {
            OrderType toSortBy = orderTypeList.get(i);
            Attribute attr = toSortBy.getAttribute();
            int attrIndex = schema.indexOf(attr);
            int comparison = Tuple.compareTuples(tuple1, tuple2, attrIndex);
            if (comparison != 0) {
                int mult = toSortBy.getOrder() == OrderType.Order.DESC ? -1 : 1;
                return mult * comparison;
            }
        }
        return 0;
    }
}