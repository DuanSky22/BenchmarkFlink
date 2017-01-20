package com.duansky.benchmark.flink.test.scripts;

import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.graph.Graph;

import java.io.File;

/**
 * Created by DuanSky on 2016/12/21.
 */
public class TestMemoryUse extends AbstractScript {

    public static String resPath = Contract.BASE_FOLD + File.separator + "test-memory-use.txt";

    public static String name = "test memory use";

    public TestMemoryUse(){
        super();
        setResPath(resPath);
        setScriptName(name);
    }

    public TestMemoryUse(String templatePath){
        super(templatePath);
        setResPath(resPath);
        setScriptName(name);
    }

    public static void main(String args[]) throws Exception{
        TestMemoryUse test =  (args != null && args.length == 1) ?
                new TestMemoryUse(args[0])
                : new TestMemoryUse();
        test.run();
    }


    @Override
    protected String runInternal(GraphTemplate template) throws Exception{
        Graph graph = graphGenerator.generateGraph(env,
                transformer.getEdgePath(Contract.DATA_FOLDER_GELLY,template),
                transformer.getVertexPath(Contract.DATA_FOLDER_GELLY,template));
        return template + " => the edge size is " + graph.numberOfEdges();
    }

}
