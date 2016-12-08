package com.duansky.benchmark.flink.analysis.datastream.transformations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment();
        senv.getConfig().disableSysoutLogging();
        senv.setParallelism(2);

        int size = 10;
        List<Integer> list = new ArrayList<Integer>(100);
        for(int i = 0; i < size; i++) list.add(i);

        DataStream<Integer> dataStream = senv.fromCollection(list);
        dataStream
                .broadcast()
                .print();
        senv.execute();
    }
}
