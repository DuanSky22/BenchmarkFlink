package com.duansky.benchmark.flink.test.dataset.transformations;

import com.duansky.benchmark.flink.test.dataset.transformations.pojo.WC;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _Reduce {

    /**
     * ReduceFunction that sums Integer attributes of a POJO
     */
    public static class WordCounter implements ReduceFunction<WC>{
        public WC reduce(WC value1, WC value2) throws Exception {
            return new WC(value1.getWord(), value1.getCount() + value2.getCount());
        }
    }

    public static class MyGroupReduce implements GroupReduceFunction<WC,WC> {
        @Override
        public void reduce(Iterable<WC> values, Collector<WC> out) throws Exception {
            Map<String,Integer> map = new HashMap<>();
            for(WC value : values) {
                map.put(value.getWord(), map.getOrDefault(value.getWord(), 0) + value.getCount());
            }
            map.forEach((s, integer) -> out.collect(new WC(s,integer)));
        }
    }

    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        List<WC> list = new ArrayList<WC>(10);
        list.add(new WC("a",1)); list.add(new WC("b",1));
        list.add(new WC("b",2)); list.add(new WC("b",1));
        DataSet<WC> words = env.fromCollection(list);

        words
                .groupBy("word","count")// DataSet grouping on field "word"
                .reduce(new WordCounter())// apply ReduceFunction on grouped DataSet
//                .reduceGroup(new MyGroupReduce())
                .print()
        ;
    }
}
