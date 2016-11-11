package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/11.
 */
public class _Aggregate {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<Tuple4<Integer,String,Double,Double>> list = new ArrayList<>(10);
        list.add(new Tuple4<>(1,"a",0.2,0.4)); list.add(new Tuple4<>(1,"a",0.2,0.3));
        list.add(new Tuple4<>(3,"a",0.4,0.4)); list.add(new Tuple4<>(1,"b",0.1,0.2));
        DataSet<Tuple4<Integer,String,Double,Double>> dataSet = env.fromCollection(list);

        dataSet
//                .groupBy(1)
                .minBy(0,2)
//                .sum(0)
//                .aggregate(Aggregations.SUM,0)
//                .and(Aggregations.MIN,2)
//                .and(Aggregations.MAX,2)
                .print()
        ;

    }
}
