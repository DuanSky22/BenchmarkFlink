package com.duansky.benchmark.flink.analysis.dataset.transformations;

import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/15.
 */
public class _Union {

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        env.getConfig().disableSysoutLogging();

        int size = 4;
        List<Integer> a = new ArrayList<>(size);
        for(int i = 0; i < size; i++){
            a.add(i);
        }

        List<Integer> b = new ArrayList<>(size);
        for(int i = 0; i < size; i++){
            b.add(size+i);
        }

        DataSet<Integer> dataSet1 = env.fromCollection(a);
        DataSet<Integer> dataSet2 = env.fromCollection(b);

        DataSet<Integer> union = dataSet1.union(dataSet2);

        union.print();
    }
}
