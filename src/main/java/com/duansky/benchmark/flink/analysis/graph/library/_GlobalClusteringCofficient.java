package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient;
import org.apache.flink.graph.library.clustering.undirected.TriangleCount;
import org.apache.flink.graph.library.metric.undirected.VertexMetrics;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/11/21.
 */
public class _GlobalClusteringCofficient {
    public static void main(String[] args) throws Exception{
        Graph<IntValue,NullValue,NullValue> input = Graphs.createGraph();
        test(input);
    }

    public static void test(Graph<IntValue,NullValue,NullValue> input) throws Exception{
        GlobalClusteringCoefficient gcc = (GlobalClusteringCoefficient)input.run(new GlobalClusteringCoefficient<>());
        input.getContext().execute();
        System.out.println(gcc.getResult().getGlobalClusteringCoefficientScore());
    }

    public static void analysis(Graph<IntValue,NullValue,NullValue> input) throws Exception{
        //calculate the triangle count.
        TriangleCount<IntValue,NullValue,NullValue> triangleCount = new TriangleCount<>();
        input.run(triangleCount);
        triangleCount.getResult().longValue();

        VertexMetrics<IntValue,NullValue, NullValue> vertexMetrics = new VertexMetrics<>();
        input.run(vertexMetrics);
    }
}
