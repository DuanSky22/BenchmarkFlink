package com.duansky.benchmark.flink.analysis.graph.pregel;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/8.
 */
public class _VertexCentricIterations {

    public static long src;

    public static void main(String args[]){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
        env.getConfig().disableSysoutLogging();
        testVertexCentricIterations(env);
    }

    public static void testVertexCentricIterations(ExecutionEnvironment env){
        src = 1;                        //define the original vertex.
        Graph graph = Graphs.createGraphForIter0(env); //define the graph.
        try {
            graph.runVertexCentricIteration(new SSSPComputeFunction(),new SSPMessageCombiner(),10)
                    .getVertices().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class SSSPComputeFunction extends ComputeFunction<LongValue,DoubleValue,DoubleValue,DoubleValue>{

        @Override
        public void compute(Vertex<LongValue, DoubleValue> vertex, MessageIterator<DoubleValue> doubleValues) throws Exception {
            /**
             * vertex : receiving data vertex.
             * DoubleValues:the values collected for this vertex.
             */
            //if the current vertex is the source vertex, the min distance is zero, other init it with positive infinity.
            Double minDistance = vertex.getId().getValue() == src ? 0d : Double.POSITIVE_INFINITY;

            //combine all receiving message that choose the min distance.
            for(DoubleValue message : doubleValues){
                minDistance = Math.min(message.getValue(),minDistance);
            }

            //if the min distance is smaller than its original value
            if(minDistance < vertex.getValue().getValue()){
                //set this min distance to its current value.
                setNewVertexValue(new DoubleValue(minDistance));
                //and send message to its neighborhood.
                for(Edge<LongValue,DoubleValue> e : getEdges()){
                    sendMessageTo(e.getTarget(),new DoubleValue(minDistance + e.getValue().getValue()));
                }
            }
        }
    }

    public static final class SSPMessageCombiner extends MessageCombiner<LongValue,DoubleValue>{

        @Override
        public void combineMessages(MessageIterator<DoubleValue> DoubleValues) throws Exception {
            Double minMessage = Double.POSITIVE_INFINITY;
            for(DoubleValue message : DoubleValues){
                minMessage = Math.min(message.getValue(),minMessage);
            }
            //Sends the combined message to the target vertex.
            sendCombinedMessage(new DoubleValue(minMessage));
        }
    }
}
