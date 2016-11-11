package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _MapPartition {

    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableSysoutLogging();

        List<String> lines = new ArrayList<String>(5);
        lines.add("hello world");
        lines.add("world war");
        lines.add("war broken");
        lines.add("broken friendship");
        lines.add("friendship end");

        DataSet<String> dataSet = env.fromCollection(lines);
        dataSet.mapPartition(new PartitionCounter()).print();
    }

    public static class PartitionCounter implements MapPartitionFunction<String,Long>{

        public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception {
            long res = 0;
            for(String value : values){
                System.out.println(value);
                res++;
            }
            out.collect(res);
        }
    }
}
