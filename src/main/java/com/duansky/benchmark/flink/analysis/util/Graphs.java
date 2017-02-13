package com.duansky.benchmark.flink.analysis.util;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.simple.directed.Simplify;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/21.
 */
public class Graphs {

    public static Graph<IntValue,NullValue,NullValue> createGraph(){
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        String edgePath = Graphs.class.getClassLoader().getResource("") + File.separator + "edges.txt";
        Graph<IntValue,NullValue,NullValue> input =
                null;
        try {
            input = (Graph<IntValue, NullValue, NullValue>) generator.generateGraph(env,edgePath).getUndirected().run(new Simplify());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return input;
    }

    /**
     * create a graph.
     * The graph looks like
     * 1 —— 2 —— 5
     * |        |
     * 3   ——  4    6
     *
     * @param env
     * @return a graph
     */
    public static Graph<LongValue,DoubleValue,DoubleValue> createGraphForIter(ExecutionEnvironment env){
        int vertexCount = 6, edgeCount = 5;
        List<Vertex<LongValue,DoubleValue>> verties = new ArrayList<Vertex<LongValue, DoubleValue>>(vertexCount);
        for(long i = 1; i <= vertexCount; i++){
            verties.add(new Vertex<LongValue,DoubleValue>(new LongValue(i),new DoubleValue(Double.POSITIVE_INFINITY)));
        }
        List<Edge<LongValue,DoubleValue>> edges = new ArrayList<Edge<LongValue, DoubleValue>>(edgeCount);
        edges.add(new Edge<LongValue, DoubleValue>(new LongValue(1l),new LongValue(2l),new DoubleValue(2d)));
        edges.add(new Edge<LongValue, DoubleValue>(new LongValue(1l),new LongValue(3l),new DoubleValue(3d)));
        edges.add(new Edge<LongValue, DoubleValue>(new LongValue(2l),new LongValue(5l),new DoubleValue(5d)));
        edges.add(new Edge<LongValue, DoubleValue>(new LongValue(3l),new LongValue(4l),new DoubleValue(20d)));
        edges.add(new Edge<LongValue, DoubleValue>(new LongValue(5l),new LongValue(4l),new DoubleValue(4d)));

        return Graph.fromCollection(verties,edges,env);
    }

    public static Graph<Long,Double ,Double > createGraphForIter0(ExecutionEnvironment env){
        int vertexCount = 6, edgeCount = 5;
        List<Vertex<Long ,Double >> verties = new ArrayList<Vertex<Long , Double >>(vertexCount);
        for(long i = 1; i <= vertexCount; i++){
            verties.add(new Vertex<Long ,Double >(new Long (i),new Double (Double.POSITIVE_INFINITY)));
        }
        List<Edge<Long ,Double >> edges = new ArrayList<Edge<Long , Double >>(edgeCount);
        edges.add(new Edge<Long , Double >(new Long (1l),new Long (2l),new Double (2d)));
        edges.add(new Edge<Long , Double >(new Long (1l),new Long (3l),new Double (3d)));
        edges.add(new Edge<Long , Double >(new Long (2l),new Long (5l),new Double (5d)));
        edges.add(new Edge<Long , Double >(new Long (3l),new Long (4l),new Double (20d)));
        edges.add(new Edge<Long , Double >(new Long (5l),new Long (4l),new Double (4d)));

        return Graph.fromCollection(verties,edges,env);
    }

    public static void printGraph(Graph graph){
        try {
            System.out.println("=================graph======================");
            System.out.println("  number of edges.txt:"+graph.numberOfEdges());
            System.out.println("number of verties:"+graph.numberOfVertices());
            System.out.println("   vertie degrees:");
            graph.getDegrees().print();
            System.out.println("       verties id:");
            graph.getVertexIds().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
