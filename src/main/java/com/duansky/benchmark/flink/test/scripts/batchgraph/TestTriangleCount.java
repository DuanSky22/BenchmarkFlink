package com.duansky.benchmark.flink.test.scripts.batchgraph;

import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.components.impl.DefaultTemplate;
import com.duansky.benchmark.flink.test.scripts.AbstractScript;
import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.directed.TriangleCount;

import java.io.File;

/**
 * Created by DuanSky on 2016/12/22.
 */
public class TestTriangleCount extends AbstractScript {

    public static String resPath = Contract.BASE_FOLD + File.separator + "test-triangle-count.txt";

    public static String name = "test triangle count";

    public TestTriangleCount() {
        super();
        setResPath(resPath);
        setScriptName(name);
    }

    public TestTriangleCount(GraphTemplate template){
        super(new GraphTemplate[]{template});
        setResPath(resPath);
        setScriptName(name);
    }

    public TestTriangleCount(String templatePath){
        super(templatePath);
        setResPath(resPath);
        setScriptName(name);
    }

    @Override
    protected String runInternal(GraphTemplate template) throws Exception {
        try {
            //generate the graph of this template.
            Graph graph = graphGenerator.generateGraph(env,
                    transformer.getEdgePath(Contract.DATA_FOLDER_GELLY,template),
                    transformer.getVertexPath(Contract.DATA_FOLDER_GELLY,template));
            //run algorithm on this graph.
            TriangleCount tc = (TriangleCount) graph.run(new TriangleCount());

            //trigger this algorithm.
            env.execute("triangle count");

            //get the job result and its id.
            JobExecutionResult result = env.getLastJobExecutionResult();
            String jobId = result.getJobID().toString();

            return String.format("test for graph(%s,%s)\t%s\t%s\t%s\n",
                    template.getVertexNumber(),
                    template.getProbability(),
                    jobId,
                    tc.getResult(),
                    result.getNetRuntime());

        } catch (Exception e) {
            e.printStackTrace();

            return String.format("test for graph(%s,%s)\t%s\n",
                    template.getVertexNumber(),
                    template.getProbability(),
                    "Error!");
        }
    }

    public static void main(String args[]) throws Exception{
        TestTriangleCount test;
        if(args == null || args.length == 0) test = new TestTriangleCount();
        else if(args.length == 1) test = new TestTriangleCount(args[0]);
        else if(args.length == 2) test = new TestTriangleCount(new DefaultTemplate(Integer.parseInt(args[0]),Double.parseDouble(args[1])));
        else throw new IllegalArgumentException("Invalid Parameter");
        test.run();
    }
}
