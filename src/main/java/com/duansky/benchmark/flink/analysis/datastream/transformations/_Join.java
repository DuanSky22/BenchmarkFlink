package com.duansky.benchmark.flink.analysis.datastream.transformations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _Join {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment();
        senv.getConfig().disableSysoutLogging();

        int size = 100;
        List<Integer> list = new ArrayList<Integer>(100);
        for(int i = 0; i < size; i++) list.add(i);

        DataStream<Integer> dataStream = senv.fromCollection(list);
        dataStream
                .join(dataStream)
                .where(data -> data)
                .equalTo(data -> data)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((x,y)->x+y);

    }
}
