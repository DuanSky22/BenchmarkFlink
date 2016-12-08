package com.duansky.benchmark.flink.analysis.util;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.analysis.graph.library._LocalClusteringCofficient;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.simple.directed.Simplify;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import java.io.File;

/**
 * Created by DuanSky on 2016/11/21.
 */
public class Graphs {

    public static Graph<IntValue,NullValue,NullValue> createGraph(){
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        String edgePath = Graphs.class.getClassLoader().getResource("") + File.separator + "edges.txt";
        Graph<IntValue,NullValue,NullValue> input =
                null;
        try {
            input = (Graph<IntValue, NullValue, NullValue>) generator.generateGraph(env,edgePath).getUndirected().run(new Simplify());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return input;
    }
}
