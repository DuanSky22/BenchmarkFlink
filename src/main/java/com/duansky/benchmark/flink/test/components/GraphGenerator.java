package com.duansky.benchmark.flink.test.components;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

/**
 * Graph Generator is used to generate the specific graph directly from the
 * {@link GraphTemplate} or from a csv file.
 * Created by DuanSky on 2016/10/29.
 */
public interface GraphGenerator {

    /**
     * generate a graph directly from a {@link GraphTemplate}.
     * @param env the execution environment.
     * @param template the graph template.
     * @return a graph defined by this graph template.
     */
    Graph generateGraph(ExecutionEnvironment env,GraphTemplate template);

    /**
     * generate a graph from a csv file of edge and vertex.
     * @param env the execution environment.
     * @param edgePath the csv file path which contains the edges.txt.
     * @param vertexPath the csv file path which contains the verties.
     * @return a graph defined by this csv file.
     */
    Graph generateGraph(ExecutionEnvironment env,String edgePath,String vertexPath);

    /**
     * generate a graph from an edge file.
     * @param env the execution environment.
     * @param edgePath the edge file path.
     * @return a graph defined by this csv file.
     */
    Graph generateGraph(ExecutionEnvironment env,String edgePath);
}
