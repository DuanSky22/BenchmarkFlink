package com.duansky.benchmark.flink.test.components.impl;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.components.GraphWriter;
import com.duansky.benchmark.flink.test.components.PathTransformer;
import com.duansky.benchmark.flink.test.util.Contract;
import com.duansky.benchmark.flink.test.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.junit.Test;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class DefaultGraphGeneratorTest {

    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
//    ExecutionEnvironment env = ExecutionEnvironment.
//        createRemoteEnvironment(Contract.host,Contract.port);

    @Test
    public void testDefaultGraphGenerator(){
        env.getConfig().disableSysoutLogging();
        GraphTemplate template = new DefaultTemplate(10,0.6);
        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        Graph graph = generator.generateGraph(env,template);
        Graphs.printGraph(graph);
    }

    @Test
    public void testFilePermission(){
        env.getConfig().disableSysoutLogging();

        String folder = Contract.DATA_FOLDER_GELLY;

        GraphTemplate template = new DefaultTemplate(12,0.6);

        GraphWriter writer = DefaultGraphWriter.getInstance();
        writer.writeAsFile(folder,template);

        PathTransformer transformer = DefaultPathTransformer.getInstance();

        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        Graph graph = generator.generateGraph(env,
                transformer.getEdgePath(folder,template),
                transformer.getVertexPath(folder,template));

        Graphs.printGraph(graph);
    }

}