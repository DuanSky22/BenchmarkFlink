package com.duansky.benchmark.flink.paper;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by DuanSky on 2016/12/8.
 */
public class _PageRank {

    public static void main(String[] args) throws Exception{
        //csv 文件交互 datastream -> dataset.
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String edgePath = "";
//        Graph graph = Graph.fromCsvReader(edgePath,env).keyType(Integer.class);
//        env.execute();

    }
}
