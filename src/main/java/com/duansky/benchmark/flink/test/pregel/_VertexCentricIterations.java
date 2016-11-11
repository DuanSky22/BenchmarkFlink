package com.duansky.benchmark.flink.test.pregel;

import com.duansky.benchmark.flink.util.Contract;
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
    public static Graph<LongValue,DoubleValue,DoubleValue> createGraph(ExecutionEnvironment env){
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

    public static void testVertexCentricIterations(ExecutionEnvironment env){
        src = 1;                        //define the original vertex.
        Graph graph = createGraph(env); //define the graph.
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
             * vertex :reciving data vertex.
             * DoubleValues:the values collected for this vertex.
             */
            Double minDistance = vertex.getId().getValue() == src ? 0d : Double.POSITIVE_INFINITY;
            for(DoubleValue message : doubleValues){
                minDistance = Math.min(message.getValue(),minDistance);
            }
            if(minDistance < vertex.getValue().getValue()){
                setNewVertexValue(new DoubleValue(minDistance));
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
