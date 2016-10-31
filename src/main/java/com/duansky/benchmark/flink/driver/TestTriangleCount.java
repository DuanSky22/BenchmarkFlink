package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.components.impl.DefaultTemplate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class TestTriangleCount {

    //    public static int[] vertexNums = {10,100};
    public static int[] vertexNums = {10,100,1000,10000,100000,1000000,10000000,100000000};
    public static double[] probabilitys = {0.1,0.2,0.5,0.8,1.0};



    public static GraphGenerator graphGenerator = new DefaultGraphGenerator();

    public static void main(String args[]) throws InterruptedException {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        env.getConfig().disableSysoutLogging();

        GraphTemplate[] templates = generateTempletes();
        for(GraphTemplate template : templates){
            run(env,template,new TriangleListing());
            Thread.sleep(5000);
        }

    }

    public static GraphTemplate[] generateTempletes(){
        int vn = vertexNums.length, pn = probabilitys.length, total = vn * pn;
        GraphTemplate[] templates = new GraphTemplate[total];
        int curr = 0;
        for(int i = 0; i < vertexNums.length; i++){
            for(int j = 0; j <probabilitys.length; j++){
                templates[curr++] = new DefaultTemplate(vertexNums[i],probabilitys[j]);
            }
        }
        return templates;
    }


    public static void run(ExecutionEnvironment env,GraphTemplate template, GraphAlgorithm algorithm) {
        try {

            long t1 = System.currentTimeMillis();
            Graph graph = graphGenerator.generateGraph(env,template);
            graph.numberOfVertices(); //trigger the graph generation.
            long t2 = System.currentTimeMillis();

            long t3 = System.currentTimeMillis();
            DataSet<Tuple3<Integer,Integer,Integer>> res =
                    (DataSet<Tuple3<Integer,Integer,Integer>>) graph.run(algorithm);
            res.print(); //trigger the algorithm.
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
