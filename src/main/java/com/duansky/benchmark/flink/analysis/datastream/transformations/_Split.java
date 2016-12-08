package com.duansky.benchmark.flink.analysis.datastream.transformations;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _Split {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment();
        senv.getConfig().disableSysoutLogging();

        int size = 100;
        List<Integer> list = new ArrayList<Integer>(100);
        for(int i = 0; i < size; i++) list.add(i);

        DataStream<Integer> dataStream = senv.fromCollection(list);
        dataStream.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> out = new ArrayList<String>(2);
                if(value % 2 == 0) out.add("even");
                else out.add("odd");
                return out;
            }
        });
    }
}
