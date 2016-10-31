package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.junit.Test;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class DefaultGraphGeneratorTest {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void testDefaultGraphGenerator(){
        env.getConfig().disableSysoutLogging();
        GraphTemplate template = new DefaultTemplate(10,0.6);
        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        Graph graph = generator.generateGraph(env,template);
        Graphs.printGraph(graph);
    }

}