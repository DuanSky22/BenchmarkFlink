package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphContext;
import com.duansky.benchmark.flink.components.GraphGenerator;
import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.components.impl.DefaultTemplate;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class TestTriangleCount {

    public static int[] vertexNums = {10,100,1000,10000,100000,1000000,10000000,100000000};
    public static double[] probabilitys = {0.1,0.2,0.5,0.8,1.0};

    public static void main(String args[]){

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        GraphTemplate[] templates = generateTempletes();
        GraphGenerator generator = new DefaultGraphGenerator(templates);
        GraphContext context = new GraphContext(generator);
        Driver driver = new Driver(env,context,new TriangleListing());
        driver.go();
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


}
