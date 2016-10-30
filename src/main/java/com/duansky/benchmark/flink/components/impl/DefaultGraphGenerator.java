package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.util.Graphs;
import com.duansky.benchmark.flink.util.Maths;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class DefaultGraphGenerator implements GraphGenerator{

    private GraphTemplate[] templates;

    public DefaultGraphGenerator(){super();}

    public DefaultGraphGenerator(GraphTemplate... templates){
        super();
        this.templates = templates;
    }

    public boolean setGraphTemplete(GraphTemplate... template) {
        this.templates = template;
        return true;
    }

    public Graph[] generateGraph(ExecutionEnvironment env) {
        if(templates == null)
            throw new IllegalArgumentException("the templete must be inited by using DefaultTemplate first!");
        Graph[] graphs = new Graph[templates.length]; int i = 0;
        for (GraphTemplate template : templates) {
            int n = template.getVertexNumber();
            double p = template.getProbability();
            //the random edge.
            int[][] e = Maths.getRandomUndirectedPairs(n, (int)(Maths.getCombinationsNumber(n,2) * p));
            DataSet<Edge<Integer,NullValue>> edges = Graphs.transform(env,e);
            graphs[i++] = Graph.fromDataSet(edges,env);
        }

        return graphs;
    }
}
