package com.duansky.benchmark.flink.analysis.graph.generator;

import com.duansky.benchmark.flink.test.util.Graphs;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;

/**
 * Created by DuanSky on 2016/11/7.
 */
public class _RMatGraph {

    public static void main(String args[]){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        testRMatGraph(env);
    }

    public static void testRMatGraph(ExecutionEnvironment env){

        RandomGenerableFactory<JDKRandomGenerator> factory = new JDKRandomGeneratorFactory();

        long vertexCount = 1000;
        long edgeCount = 100;

        Graph graph = new RMatGraph(env,factory,vertexCount,edgeCount).generate();

        Graphs.printGraph(graph);
    }
}
