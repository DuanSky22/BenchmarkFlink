package com.duansky.benchmark.flink.analysis.dataset.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _Iterations {

    static int size = 1000000;

    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        getPi(env);
    }

    public static void getPi(ExecutionEnvironment env) throws Exception {
        DataSet<Integer> dataSet = env.fromElements(0); //the initial data set. Actually its used for counter.
        IterativeDataSet<Integer> iter = dataSet.iterate(size); // the initial iterative data set.
        DataSet<Integer> middleDataSet = iter.map((MapFunction<Integer, Integer>) (Integer value) -> {
            double x = Math.random();
            double y = Math.random();
            return value + ((x * x + y * y < 1) ? 1 : 0);
        }); //if the random point(x,y) fill in the unit circle, the counter increase by 1.
        DataSet<Integer> result = iter.closeWith(middleDataSet); // use the middle data set as the iterative middle result.
        result.map((MapFunction<Integer, Double>) value -> value * 4.0 / size).print(); // get the pi.
    }
}
