package com.duansky.benchmark.flink.analysis.datastream.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/16.
 */
public class _Window {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment();
        senv.getConfig().disableSysoutLogging();

        int size = 100;
        List<Integer> list = new ArrayList<Integer>(100);
        for(int i = 0; i < size; i++) list.add(i);

        DataStream<Integer> dataStream = senv.fromCollection(list);

        dataStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                });

        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Integer>() {
            @Override
            public long extractAscendingTimestamp(Integer element) {
                return element;
            }
        });

        dataStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Integer, Integer, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple integer, TimeWindow window, Iterable<Integer> input, Collector<Integer> out) throws Exception {
                        int sum = 0;
                        for(int i : input)
                            sum+=i;
                        out.collect(sum);
                    }
                });
    }
}
