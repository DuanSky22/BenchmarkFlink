package com.duansky.benchmark.flink.analysis.graph.pregel;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

/**
 * Created by DuanSky on 2016/12/8.
 */
public class _ScatterGatherIteration {


    public static void main(String args[]){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        testScatterGather(env);
    }

    public static void testScatterGather(ExecutionEnvironment env){
        Graph<Long,Double,Double> graph = Graphs.createGraphForIter0(env);
        Graph<Long,Double,Double> res = graph.runScatterGatherIteration(new MyScatter(),new MyGather(),100);
        Graphs.printGraph(res);
    }


    public static final class MyScatter extends ScatterFunction<Long, Double, Double, Double> {
        @Override
        public void sendMessages(Vertex<Long, Double> vertex) throws Exception {
            for(Edge<Long,Double> edge : getEdges()){
                sendMessageTo(edge.getTarget(),vertex.getValue() + edge.getValue());
            }
        }
    }

    public static final class MyGather extends GatherFunction<Long,Double,Double>{
        @Override
        public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) throws Exception {
            Double minDistance = Double.POSITIVE_INFINITY;
            for(Double curr : inMessages){
                minDistance = Math.min(minDistance,curr);
            }
            if(minDistance < vertex.getValue()){
                setNewVertexValue(minDistance);
            }
        }
    }
}
