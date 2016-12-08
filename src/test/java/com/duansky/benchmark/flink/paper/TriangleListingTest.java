package com.duansky.benchmark.flink.paper;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

/**
 * Created by DuanSky on 2016/12/8.
 */
public class TriangleListingTest {

    private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    private Graph<IntValue,NullValue,NullValue> graph = Graphs.createGraph();

    @Test
    public void testTriangleListing(){
        try {
            TriangleListing<IntValue,NullValue,NullValue> triangleListing = new TriangleListing();
            DataSet<Tuple3<IntValue,IntValue,IntValue>> res1 = (DataSet<Tuple3<IntValue,IntValue,IntValue>>) graph.run(triangleListing);
            res1.print();
            DataSet<Tuple3<IntValue,IntValue,IntValue>> res =
                    triangleListing.addEdge(new Edge<IntValue,NullValue>(new IntValue(1),new IntValue(2),new NullValue()));
            System.out.println("=============");
            res.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
