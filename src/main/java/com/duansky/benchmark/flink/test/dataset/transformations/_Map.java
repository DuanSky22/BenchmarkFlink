package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _Map {

    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        int size = 10;
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>(size);
        for(int i = 0; i < size; i++)
            list.add(new Tuple2<Integer, Integer>(i,i));
        DataSet<Tuple2<Integer,Integer>> dataSet = env.fromCollection(list);
        DataSet<Integer> mapRes = dataSet.map(new MyMapFunction());
        mapRes.print();
    }

    public static class MyMapFunction implements MapFunction<Tuple2<Integer,Integer>,Integer>{
        public Integer map(Tuple2<Integer, Integer> value) throws Exception {
            return value.f0 + value.f1;
        }
    }
}
