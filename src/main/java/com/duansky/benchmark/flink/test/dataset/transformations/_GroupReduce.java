package com.duansky.benchmark.flink.test.dataset.transformations;

import com.duansky.benchmark.flink.test.dataset.transformations.pojo.WC;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _GroupReduce {


    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<Tuple2<Integer,String>> list = new ArrayList<>(4);
        list.add(new Tuple2<>(1,"a")); list.add(new Tuple2<>(1,"a"));
        list.add(new Tuple2<>(2,"a")); list.add(new Tuple2<>(2,"b"));

        DataSet<Tuple2<Integer,String >> dataSet = env.fromCollection(list);

        dataSet
                .groupBy(0,1)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new DistinctReduce())
                .print()
                ;

    }

    public static class DistinctReduce implements GroupReduceFunction<Tuple2<Integer,String>,Tuple2<Integer,String>>{
        @Override
        public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
            Integer key = null;
            Set<String> set = new HashSet<>();
            for (Tuple2<Integer, String> value : values) {
                key = value.f0;
                set.add(value.f1);
            }
            for (String s : set) {
                out.collect(new Tuple2<>(key,s));
            }
        }
    }


}
