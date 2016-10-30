package com.duansky.benchmark.flink.components;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class GraphContext {

    private GraphGenerator graphGenerator;

    public GraphContext(){super();};

    public GraphContext(GraphGenerator generator){
        this.graphGenerator = generator;
    }

    public void setGraphGenerator(GraphGenerator graphGenerator){
        this.graphGenerator = graphGenerator;
    }

    public Graph[] generateGraph(ExecutionEnvironment env){
        return graphGenerator.generateGraph(env);
    }
}
