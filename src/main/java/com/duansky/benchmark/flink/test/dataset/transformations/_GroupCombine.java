package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _GroupCombine {
    public static void main(String args[]) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<String> list = new ArrayList<>(10);
        list.add("a"); list.add("b"); list.add("c"); list.add("d"); list.add("e");
        list.add("a"); list.add("f"); list.add("c"); list.add("g"); list.add("e");

        DataSet<String> dataSet = env.fromCollection(list);
        dataSet
                .groupBy((KeySelector<String, String>) value -> value)
                .combineGroup(new GroupCombineFunction<String, Tuple2<String,Integer>>() {

                    @Override
                    public void combine(Iterable<String> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String key = null; int count = 0;
                        for(String value : values){
                            key = value;
                            count ++;
                        }
                        out.collect(new Tuple2<>(key,count));
                    }
                })
                .groupBy(0) //pay attention! we need groupBy again here!
                .reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, String>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<String> out) throws Exception {
                        String key = null; int count = 0;
                        for (Tuple2<String, Integer> value : values) {
                            key = value.f0;
                            count += value.f1;
                        }
                        out.collect(key + ":" + count);
                    }
                })
                .print()
        ;
    }
}
