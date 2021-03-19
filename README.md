# CS3223 Database Systems Implementation Project

**_AY2020/2021 Semester 2<br>
School of Computing<br>
National University of Singapore_**

This project focuses on the implementation of a simple SPJ _(Select-Project-Join)_ query engine to illustrate how query processing works in modern database management systems (DBMS), specifically relational databases (RDBMS).


## Implementation Outline

Based on the project requirements, we have implemented the following operators in this SPJ query engine:
- Block Nested Loops Join (see [BlockNestedJoin.java](src/qp/operators/BlockNestedJoin.java))
- Sort Merge join (see [SortMergeJoin.java](src/qp/operators/SortMergeJoin.java))
- External sort (see [Sort.java](src/qp/operators/Sort.java))
- `DISTINCT` operator (see [Distinct.java](src/qp/operators/Distinct.java))
- `GROUPBY` operator (see [GroupBy.java](src/qp/operators/GroupBy.java))
- `ORDERBY` operator (see [OrderBy.java](src/qp/operators/OrderBy.java))
- Aggregate operations: `MIN`, `MAX`, `COUNT` and `AVG` operators (see [Aggregate.java](src/qp/operators/Aggregate.java) and [AggregateAttribute.java](src/qp/operators/AggregateAttribute.java))

### BlockNestedJoin
The BlockNestedJoin operator uses |B|-2 buffers to store as many pages of tuples of the left relation as possible. It uses another 1 buffer for a single page of the right relation and another 1 buffer for output. In total, it uses |B| buffers available. It reads the left relation pages into a LinkedList of pages. Then, it flattens all the tuples across these pages into an ArrayList for comparison with the single right page currently loaded in for comparison. While the output buffer is not full, we compare the flattened list of left tuples with all |S| pages of the right relation S. When |S| has been exhausted, we load in the next block of left pages following the aforementioned algorithm and repeat. Every time the output buffer page is full, we return it in the `.next()` function, and also store the current pointers for both the left and right pointers for the tuples, so that the algorithm can continue where it left off when `.next()` is called again for the next output page.

### Sort
A Sort utility class was created. The Sort class is used by SortMergeJoin, Distinct and Orderby Operators. It is the implementation of external sorting. In the first pass, the buffers would be used to create sorted runs. We used the OrderBy comparator to order the tuples. Each sorted run is written into a Java temporary file and would be deleted at the end of the program. We have used Java ObjectOutputStream to write the tuples in the files. Then, the sorted runs will be continuously merged until only one sorted run is left. The number of sorted runs that can be merged at any one time is number of |B| – 1 as one is used for output. We use maintain a sorted array of |B|– 1 size to know which tuple should be chosen to place into the output buffer and another tuple from the same sorted run will be added to the sorted array. For simplicity, we just returned the Java ObjectInputStream of the entire sorted table instead of return by batches. 

### SortMergeJoin
The SortMergeJoin operator uses the Sort utility class to sort the left and right tables according to the condition list. We maintain indexes of the two tables when checking if the tuples can join under the condition. At any point of time, each table will use (|B| - 2) / 2 buffers as one buffer is used for output and one to hold tuples for backtracking. We compare the tuples that the indexes are pointing to know which index to increment. For backtracking, we also have an ArrayList to buffer the tuples and we assume the buffered tuples to be able to fit into one buffer as it was hard to maintain a last in first out structure. The SortMergeJoin outputs to the next operator by batches.

### Distinct
The Distinct operator also uses the Sort utility class. However, the Sort instance is special as the isDistinct is set to True. The OrderBy comparator used in Sort class consists of a condition list that is all the attributes of the tuple. The removal of the duplicate tuples is done during sorting to reduce the intermediate table size. By doing so, when we generate the sorted runs at the first pass, we remove the duplicate tuples by using a treemap. Also, during merging of the sorted files, we check if there are duplicate tuples in the sorted array that I have mentioned above.

### OrderBy
The OrderBy operator also uses the Sort utility class. It creates a Sort instance and sort the according to the attribute stated by the user.

### Cartesian
Cartesian is done by adding a special comparison type in the Condition class. This will create a special Join operator which will accept all tuples that are formed by doing a cartesian product between the left table and right table. To check if there are any cartesian to be done, we check if there are any tables that are not present the Join conditions and create the special cartesian Join operator. Any Join operator like BlockNestJoin, SortMergeJoin and NestedJoin with the special is cartesian set will accept all the tuples.

### Aggregate
The Aggregate operator works mainly within [Project.java](src/qp/operators/Project.java). If the Project operator detects any required columns contain an aggregate operation (using `Attribute.getAggType()` method), the Aggregate operator reads in the tuples, calculates the required aggregate value for each tuple and appends these columns to the original tuple to be written out.

### AggregateAttribute
A AggregateAttribute utility class was created. This class is used exclusively by the Aggregate operator. It implements the various required aggregation operations for `MIN`, `MAX`, `COUNT` and `AVG` for each aggregate attribute. This helps the Aggregate operator to calculate the aggregate value, if any, for the necessary attributes in each tuple.