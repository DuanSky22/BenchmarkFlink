package com.duansky.benchmark.flink.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class Graphs {

    /**
     * transform the edge array to DataSet type.
     * @param env the execution environment.
     * @param edges the array of edges.
     * @return the dataset of the given edge array.
     */
    public static DataSet<Edge<IntValue,NullValue>> transform(ExecutionEnvironment env, int[][] edges){
        List<Edge<IntValue,NullValue>> list = new LinkedList<Edge<IntValue, NullValue>>();
        for(int[] edge : edges)
            list.add(new Edge<IntValue, NullValue>(new IntValue(edge[0]),new IntValue(edge[1]),NullValue.getInstance()));
        return env.fromCollection(list);
    }

    public static void printGraph(Graph graph){
        try {
            System.out.println("=================graph======================");
            System.out.println("  number of edges:"+graph.numberOfEdges());
            System.out.println("number of verties:"+graph.numberOfVertices());
            System.out.println("       verties id:");
            graph.getVertexIds().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
