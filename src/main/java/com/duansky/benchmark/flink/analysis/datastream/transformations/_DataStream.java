package com.duansky.benchmark.flink.analysis.datastream.transformations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _DataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment();
        senv.getConfig().disableSysoutLogging();

        int size = 10;
        List<Integer> list = new ArrayList<>(size);
        for(int i = 0; i < size; i++) list.add(i);

        DataStream<Integer> dataStream = senv.fromCollection(list);
        dataStream.filter((data)-> (data % 2 == 0)).print();

        senv.execute();

    }
}
