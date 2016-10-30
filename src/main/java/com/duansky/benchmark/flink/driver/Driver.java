package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class Driver {

    private ExecutionEnvironment env;

    private GraphContext context;
    private GraphAlgorithm algorithm;

    public Driver(ExecutionEnvironment env){
        super();
        this.env = env;
    }

    public Driver(ExecutionEnvironment env, GraphContext context, GraphAlgorithm algorithm){
        super();
        this.env = env;
        this.context = context;
        this.algorithm = algorithm;
    }

    public void go(){
        long t1 = System.currentTimeMillis();
        Graph[] graphs = context.generateGraph(env);
        long t2 = System.currentTimeMillis();
        for(Graph graph : graphs){
            try {
                long t3 = System.currentTimeMillis();
                graph.run(algorithm);
                long t4 = System.currentTimeMillis();
                System.out.println(String.format("(%s,%s) => (%s,%s)",
                        graph.numberOfVertices(),
                        graph.numberOfEdges(),
                        (t2-t1),
                        (t4-t3)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }




}
