package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.util.Graphs;
import com.duansky.benchmark.flink.util.Maths;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class DefaultGraphGenerator implements GraphGenerator{

    public Graph generateGraph(ExecutionEnvironment env,GraphTemplate template) {
        if(template == null)
            throw new IllegalArgumentException("the templete must be inited by using DefaultTemplate first!");
        int n = template.getVertexNumber();
        double p = template.getProbability();
        //the random edge.
        int[][] e = Maths.getRandomUndirectedPairs(n, (int)(Maths.getCombinationsNumber(n,2) * p));
        DataSet<Edge<IntValue,NullValue>> edges = Graphs.transform(env,e);
        return Graph.fromDataSet(edges,env);
    }
}
