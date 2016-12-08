package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.TriangleCount;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/11/21.
 */
public class _TriangleCount {
    public static void main(String[] args) {
        Graph input = Graphs.createGraph();
        test(input);
    }

    public static void analysis(Graph<IntValue,NullValue,NullValue> input){

    }

    public static void test(Graph<IntValue,NullValue,NullValue> input){
        try {
            TriangleCount t =(TriangleCount) input.run(new TriangleCount<>());
            input.getContext().execute();
            System.out.println(t.getResult());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
