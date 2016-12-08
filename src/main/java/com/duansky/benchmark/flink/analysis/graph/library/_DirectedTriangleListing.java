package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultTemplate;
import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeOrder;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/18.
 */
public class _DirectedTriangleListing {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(Contract.host,Contract.port);
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        GraphGenerator generator = DefaultGraphGenerator.getInstance();
        Graph<IntValue,NullValue,NullValue> input =
                (Graph<IntValue, NullValue, NullValue>) generator.generateGraph(env,new DefaultTemplate(10,0.5));

//        Graphs.printGraph(input);

        input.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<>()).print();

//        input.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<>()).print();
    }

    public static void analysis(Graph<IntValue,NullValue,NullValue> input){

    }

    private static final class OrderByID<T extends Comparable<T>, ET>
            implements MapFunction<Edge<T, ET>, Tuple3<T, T, ByteValue>> {
        private ByteValue forward = new ByteValue(EdgeOrder.FORWARD.getBitmask());

        private ByteValue reverse = new ByteValue(EdgeOrder.REVERSE.getBitmask());

        private Tuple3<T, T, ByteValue> output = new Tuple3<>();

        @Override
        public Tuple3<T, T, ByteValue> map(Edge<T, ET> value)
                throws Exception {
            if (value.f0.compareTo(value.f1) < 0) {
                output.f0 = value.f0;
                output.f1 = value.f1;
                output.f2 = forward;
            } else {
                output.f0 = value.f1;
                output.f1 = value.f0;
                output.f2 = reverse;
            }

            return output;
        }
    }

    /**
     * Reduce bitmasks to a single value using bitwise-or.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("0; 1")
    private static final class ReduceBitmask<T>
            implements GroupReduceFunction<Tuple3<T, T, ByteValue>, Tuple3<T, T, ByteValue>> {
        @Override
        public void reduce(Iterable<Tuple3<T, T, ByteValue>> values, Collector<Tuple3<T, T, ByteValue>> out)
                throws Exception {
            Tuple3<T, T, ByteValue> output = null;

            byte bitmask = 0;

            for (Tuple3<T, T, ByteValue> value: values) {
                output = value;
                bitmask |= value.f2.getValue();
            }

            output.f2.setValue(bitmask);
            out.collect(output);
        }
    }

    /**
     * Removes edge values while emitting a Tuple3 where f0 and f1 are,
     * respectively, the lesser and greater of the source and target IDs
     * by degree count. If the source and target vertex degrees are equal
     * then the IDs are compared and emitted in order. The third field is
     * a bitmask representing the vertex order.
     *
     * @param <T> ID type
     * @param <ET> edge value type
     */
    private static final class OrderByDegree<T extends Comparable<T>, ET>
            implements MapFunction<Edge<T, Tuple3<ET, VertexDegrees.Degrees, VertexDegrees.Degrees>>, Tuple3<T, T, ByteValue>> {
        private ByteValue forward = new ByteValue((byte)(EdgeOrder.FORWARD.getBitmask() << 2));

        private ByteValue reverse = new ByteValue((byte)(EdgeOrder.REVERSE.getBitmask() << 2));

        private Tuple3<T, T, ByteValue> output = new Tuple3<>();

        @Override
        public Tuple3<T, T, ByteValue> map(Edge<T, Tuple3<ET, VertexDegrees.Degrees, VertexDegrees.Degrees>> value)
                throws Exception {
            Tuple3<ET, VertexDegrees.Degrees, VertexDegrees.Degrees> degrees = value.f2;
            long sourceDegree = degrees.f1.getDegree().getValue();
            long targetDegree = degrees.f2.getDegree().getValue();

            if (sourceDegree < targetDegree ||
                    (sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
                output.f0 = value.f0;
                output.f1 = value.f1;
                output.f2 = forward;
            } else {
                output.f0 = value.f1;
                output.f1 = value.f0;
                output.f2 = reverse;
            }

            return output;
        }
    }

    /**
     * Generates the set of triplets by the pairwise enumeration of the open
     * neighborhood for each vertex. The number of triplets is quadratic in
     * the vertex degree; however, data skew is minimized by only generating
     * triplets from the vertex with least degree.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("0")
    private static final class GenerateTriplets<T extends CopyableValue<T>>
            implements GroupReduceFunction<Tuple3<T, T, ByteValue>, Tuple4<T, T, T, ByteValue>> {
        private Tuple4<T, T, T, ByteValue> output = new Tuple4<>(null, null, null, new ByteValue());

        private List<Tuple2<T, ByteValue>> visited = new ArrayList<>();

        @Override
        public void reduce(Iterable<Tuple3<T, T, ByteValue>> values, Collector<Tuple4<T, T, T, ByteValue>> out)
                throws Exception {
            int visitedCount = 0;

            Iterator<Tuple3<T, T, ByteValue>> iter = values.iterator();

            while (true) {
                Tuple3<T, T, ByteValue> edge = iter.next();
                byte bitmask = edge.f2.getValue();

                output.f0 = edge.f0;
                output.f2 = edge.f1;

                for (int i = 0; i < visitedCount; i++) {
                    Tuple2<T, ByteValue> previous = visited.get(i);

                    output.f1 = previous.f0;
                    output.f3.setValue((byte)(previous.f1.getValue() | bitmask));

                    // u, v, w, bitmask
                    out.collect(output);
                }

                if (! iter.hasNext()) {
                    break;
                }

                byte shiftedBitmask = (byte)(bitmask << 2);

                if (visitedCount == visited.size()) {
                    visited.add(new Tuple2<>(edge.f1.copy(), new ByteValue(shiftedBitmask)));
                } else {
                    Tuple2<T, ByteValue> update = visited.get(visitedCount);
                    edge.f1.copyTo(update.f0);
                    update.f1.setValue(shiftedBitmask);
                }

                visitedCount += 1;
            }
        }
    }

    /**
     * Simply project the triplet as a triangle while collapsing triplet and edge bitmasks.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0; 1; 2")
    @FunctionAnnotation.ForwardedFieldsSecond("0; 1")
    private static final class ProjectTriangles<T>
            implements JoinFunction<Tuple4<T, T, T, ByteValue>, Tuple3<T, T, ByteValue>, TriangleListing.Result<T>> {
        private TriangleListing.Result<T> output = new TriangleListing.Result<>(null, null, null, new ByteValue());

        @Override
        public TriangleListing.Result<T> join(Tuple4<T, T, T, ByteValue> triplet, Tuple3<T, T, ByteValue> edge)
                throws Exception {
            output.f0 = triplet.f0;
            output.f1 = triplet.f1;
            output.f2 = triplet.f2;
            output.f3.setValue((byte)(triplet.f3.getValue() | edge.f2.getValue()));
            return output;
        }
    }

    /**
     * Reorders the vertices of each emitted triangle (K0, K1, K2, bitmask)
     * into sorted order such that K0 < K1 < K2.
     *
     * @param <T> ID type
     */
    private static final class SortTriangleVertices<T extends Comparable<T>>
            implements MapFunction<TriangleListing.Result<T>, TriangleListing.Result<T>> {
        @Override
        public TriangleListing.Result<T> map(TriangleListing.Result<T> value)
                throws Exception {
            // by the triangle listing algorithm we know f1 < f2
            if (value.f0.compareTo(value.f1) > 0) {
                byte bitmask = value.f3.getValue();

                T temp_val = value.f0;
                value.f0 = value.f1;

                if (temp_val.compareTo(value.f2) < 0) {
                    value.f1 = temp_val;

                    int f0f1 = ((bitmask & 0b100000) >>> 1) | ((bitmask & 0b010000) << 1);
                    int f0f2 = (bitmask & 0b001100) >>> 2;
                    int f1f2 = (bitmask & 0b000011) << 2;

                    value.f3.setValue((byte)(f0f1 | f0f2 | f1f2));
                } else {
                    value.f1 = value.f2;
                    value.f2 = temp_val;

                    int f0f1 = (bitmask & 0b000011) << 4;
                    int f0f2 = ((bitmask & 0b100000) >>> 3) | ((bitmask & 0b010000) >>> 1);
                    int f1f2 = ((bitmask & 0b001000) >>> 3) | ((bitmask & 0b000100) >>> 1);

                    value.f3.setValue((byte)(f0f1 | f0f2 | f1f2));
                }
            }

            return value;
        }
    }

    /**
     * Wraps the vertex type to encapsulate results from the Triangle Listing algorithm.
     *
     * @param <T> ID type
     */
    public static class Result<T>
            extends Tuple4<T, T, T, ByteValue> {
        /**
         * No-args constructor.
         */
        public Result() {}

        /**
         * Populates parent tuple with constructor parameters.
         *
         * @param value0 1st triangle vertex ID
         * @param value1 2nd triangle vertex ID
         * @param value2 3rd triangle vertex ID
         * @param value3 bitmask indicating presence of six possible edges.txt between triangle vertices
         */
        public Result(T value0, T value1, T value2, ByteValue value3) {
            super(value0, value1, value2, value3);
        }

        /**
         * Format values into a human-readable string.
         *
         * @return verbose string
         */
        public String toVerboseString() {
            byte bitmask = f3.getValue();

            return "1st vertex ID: " + f0
                    + ", 2nd vertex ID: " + f1
                    + ", 3rd vertex ID: " + f2
                    + ", edge directions: " + f0 + maskToString(bitmask, 4) + f1
                    + ", " + f0 + maskToString(bitmask, 2) + f2
                    + ", " + f1 + maskToString(bitmask, 0) + f2;
        }

        private String maskToString(byte mask, int shift) {
            switch((mask >>> shift) & 0b000011) {
                case 0b01:
                    // EdgeOrder.FORWARD
                    return "->";
                case 0b10:
                    // EdgeOrder.REVERSE
                    return "<-";
                case 0b11:
                    // EdgeOrder.MUTUAL
                    return "<->";
                default:
                    throw new IllegalArgumentException("Bitmask is missing an edge (mask = "
                            + mask + ", shift = " + shift);
            }
        }
    }
}
