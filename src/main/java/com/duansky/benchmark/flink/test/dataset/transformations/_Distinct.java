package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/11.
 */
public class _Distinct {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<Tuple3<Integer,String,Double>> list = new ArrayList<>(4);
        list.add(new Tuple3<>(1,"a",0.1)); list.add(new Tuple3<>(1,"b",0.1));
        DataSet<Tuple3<Integer,String,Double>> dataSet = env.fromCollection(list);

        dataSet
                .distinct(0,2)
                .print();
    }
}
