package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.directed.EdgeDegreesPair;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import java.io.File;

/**
 * Created by DuanSky on 2016/11/18.
 */
public class _VertexDegrees {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        String edgePath = _VertexDegrees.class.getClassLoader().getResource("")+ File.separator+"edges";
        Graph<IntValue,NullValue,NullValue> graph = generator.generateGraph(env,edgePath);

        //VertexDegrees
//        graph.run(new VertexDegrees<IntValue,NullValue,NullValue>()).print();

        //EdgeDegreesPair
        graph.run(new EdgeDegreesPair<>()).print();
    }
}
