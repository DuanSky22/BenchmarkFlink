package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _FlatMap {

    public static class MyFlatMapFunction implements FlatMapFunction<String,String>{

        public static void main(String args[]) throws Exception {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableSysoutLogging();

            List<String> lines = new ArrayList<String>(5);
            lines.add("hello world");
            lines.add("world war");
            lines.add("war broken");
            lines.add("broken friendship");
            lines.add("friendship end");

            DataSet<String> dataSet = env.fromCollection(lines);

            dataSet.flatMap(new MyFlatMapFunction()).print();
        }

        public void flatMap(String value, Collector<String> out) throws Exception {
            for(String single : value.split("\\W")) {
                out.collect(single);
            }
        }
    }
}
