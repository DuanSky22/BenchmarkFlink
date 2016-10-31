package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.components.impl.DefaultTemplate;
import com.duansky.benchmark.flink.util.Contract;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;

import java.io.File;
import java.io.PrintWriter;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class TestTriangleCount {

    public static int[] vertexNums = {1000,10000,100000,1000000};
    public static double[] probabilitys = {0.1,0.2,0.5,0.8,1.0};

    public static String outputFile = System.getProperty("user.dir")
            + File.separator + "triangle_count_test.txt";

    public static GraphGenerator graphGenerator = new DefaultGraphGenerator();

    public static void main(String args[]) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        env.getConfig().disableSysoutLogging();

        File file = new File(outputFile);
        if(!file.exists()) file.createNewFile();
        PrintWriter writer = new PrintWriter(file);

        GraphTemplate[] templates = generateTemplates();
        System.out.println("|============testing algorithm================|");
        for(GraphTemplate template : templates){
            System.out.println(String.format("start test graph(%s,%s)...", template.getVertexNumber(),template.getProbability()));
            String res = run(env,template,new TriangleListing());
            writer.write(res);
            writer.flush();

            System.out.println(String.format("test graph(%s,%s) finished! => %s",
                    template.getVertexNumber(),
                    template.getProbability(),
                    res));

            Thread.sleep(5000);
        }
        writer.close();
        System.out.println("|===========testing algorithm done.===========|");
    }

    public static GraphTemplate[] generateTemplates(){
        System.out.println("|===========generate templates================|");
        int vn = vertexNums.length, pn = probabilitys.length, total = vn * pn;
        GraphTemplate[] templates = new GraphTemplate[total];
        int curr = 0;
        for(int i = 0; i < vertexNums.length; i++){
            for(int j = 0; j <probabilitys.length; j++){
                templates[curr++] = new DefaultTemplate(vertexNums[i],probabilitys[j]);
                System.out.println(String.format("crate data source graph(%s,%s) done!", vertexNums[i],probabilitys[j]));
            }
        }
        System.out.println("|===========generate templates done===========|");
        return templates;
    }


    public static String run(ExecutionEnvironment env,GraphTemplate template, GraphAlgorithm algorithm) {
        try {
            Graph graph = graphGenerator.generateGraph(env,template);
            DataSet<Tuple3<Integer,Integer,Integer>> res =
                    (DataSet<Tuple3<Integer,Integer,Integer>>) graph.run(algorithm);
            res.print(); //trigger the algorithm.
            String jobId = env.getLastJobExecutionResult().getJobID().toString();
            JobExecutionResult result = env.getLastJobExecutionResult();

            return String.format("test for graph(%s,%s):%s=>%s\n",
                    template.getVertexNumber(),
                    template.getProbability(),
                    jobId,
                    result.getNetRuntime());

        } catch (Exception e) {
            e.printStackTrace();
            return String.format("test for graph(%s,%s):%s\n",
                    template.getVertexNumber(),
                    template.getProbability(),
                    "Error!");
        }
    }

}
