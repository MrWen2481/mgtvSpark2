package gen;

import org.spark_project.guava.collect.Range;
import org.spark_project.guava.collect.RangeMap;
import org.spark_project.guava.collect.TreeRangeMap;

/**
 * @author zyx
 * @date 2018/6/29.
 */
public class Zyx {
    public static void main(String[] args) {
        TreeRangeMap<String,String> comparableTreeRangeSet = TreeRangeMap.create();
        comparableTreeRangeSet.put(Range.closedOpen("000000", "000010"),"000000-000010");
        comparableTreeRangeSet.put(Range.closedOpen("000010", "000020"),"000010-000020");
        RangeMap<String, String> stringStringRangeMap = comparableTreeRangeSet.subRangeMap(Range.closedOpen("000000",
                "000009"));
        System.out.println(stringStringRangeMap.span());
//        System.out.println(comparableTreeRangeSet.get("000010"));
    }
}
