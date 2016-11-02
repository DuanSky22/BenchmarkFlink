package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.PathTransformer;
import com.duansky.benchmark.flink.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.components.impl.DefaultPathTransformer;
import com.duansky.benchmark.flink.util.Contract;
import com.duansky.benchmark.flink.util.Files;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;

import java.io.PrintWriter;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class TestDriverTriangleCount {

    public static String outputFile = Contract.TRIANGLE_COUNT_RESULT;

    public static GraphGenerator graphGenerator = DefaultGraphGenerator.getInstance();
    public static PathTransformer transformer = DefaultPathTransformer.getInstance();

    public static void main(String args[]) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        env.getConfig().disableSysoutLogging();

        PrintWriter writer = Files.asPrintWriter(outputFile);

        //generate graph templates.
        GraphTemplate[] templates = (args != null && args.length == 1) ?
                GraphTemplateFactory.generateTemplates(args[0]) :
                GraphTemplateFactory.generateTemplates();

        System.out.println("|============testing algorithm================|");
        for(GraphTemplate template : templates){

            System.out.println(String.format("start test graph(%s,%s)...",
                    template.getVertexNumber(),
                    template.getProbability()));

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

    public static String run(ExecutionEnvironment env,
                             GraphTemplate template,
                             GraphAlgorithm algorithm) {
        try {
            //generate the graph of this template.
            Graph graph = graphGenerator.generateGraph(env,
                    transformer.getEdgePath(Contract.DATA_FOLDER,template),
                    transformer.getVertexPath(Contract.DATA_FOLDER,template));
            //run algorithm on this graph.
            DataSet<Tuple3<Integer,Integer,Integer>> res =
                    (DataSet<Tuple3<Integer,Integer,Integer>>) graph.run(algorithm);
            //trigger this algorithm.
            res.print();
            //get the job result and its id.
            JobExecutionResult result = env.getLastJobExecutionResult();
            String jobId = result.getJobID().toString();

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
