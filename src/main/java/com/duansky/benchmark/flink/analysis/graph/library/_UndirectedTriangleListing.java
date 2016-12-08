package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/23.
 */
public class _UndirectedTriangleListing {
    public static void main(String[] args) {
        Graph<IntValue,NullValue,NullValue> graph = Graphs.createGraph();
    }

    public static void analysis(Graph<IntValue,NullValue,NullValue> input) throws Exception {
        // u, v where u < v
        DataSet<Tuple2<IntValue, IntValue>> filteredByID = input
                .getEdges()
                .flatMap(new FilterByID<IntValue, NullValue>())
                .name("Filter by ID");

        // u, v, (edge value, deg(u), deg(v))
        DataSet<Edge<IntValue, Tuple3<NullValue, LongValue, LongValue>>> pairDegree = input
                .run(new EdgeDegreePair<IntValue, NullValue, NullValue>());

        // u, v where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
        DataSet<Tuple2<IntValue, IntValue>> filteredByDegree = pairDegree
                .flatMap(new FilterByDegree<IntValue, NullValue>())
                .name("Filter by degree");

        // u, v, w where (u, v) and (u, w) are edges.txt in graph, v < w
        DataSet<Tuple3<IntValue, IntValue, IntValue>> triplets = filteredByDegree
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GenerateTriplets<IntValue>())
                .name("Generate triplets");

        // u, v, w where (u, v), (u, w), and (v, w) are edges.txt in graph, v < w
        DataSet<Tuple3<IntValue, IntValue, IntValue>> triangles = triplets
                .join(filteredByID, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
                .where(1, 2)
                .equalTo(0, 1)
                .with(new ProjectTriangles<IntValue>())
                .name("Triangle listing");
    }

    /**
     * Removes edge values while filtering such that only edges.txt where the
     * source vertex ID compares less than the target vertex ID are emitted.
     * <br/>
     * Since the input graph is a simple graph this filter removes exactly half
     * of the original edges.txt.
     *
     * @param <T> ID type
     * @param <ET> edge value type
     */
    @FunctionAnnotation.ForwardedFields("0; 1")
    private static final class FilterByID<T extends Comparable<T>, ET>
            implements FlatMapFunction<Edge<T, ET>, Tuple2<T, T>> {
        private Tuple2<T, T> edge = new Tuple2<>();

        @Override
        public void flatMap(Edge<T, ET> value, Collector<Tuple2<T, T>> out)
                throws Exception {
            if (value.f0.compareTo(value.f1) < 0) {
                edge.f0 = value.f0;
                edge.f1 = value.f1;
                out.collect(edge);
            }
        }
    }

    /**
     * Removes edge values while filtering such that edges.txt where the source
     * vertex has lower degree are emitted. If the source and target vertex
     * degrees are equal then the edge is emitted if the source vertex ID
     * compares less than the target vertex ID.
     * <br/>
     * Since the input graph is a simple graph this filter removes exactly half
     * of the original edges.txt.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("0; 1")
    private static final class FilterByDegree<T extends Comparable<T>, ET>
            implements FlatMapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple2<T, T>> {
        private Tuple2<T, T> edge = new Tuple2<>();

        @Override
        public void flatMap(Edge<T, Tuple3<ET, LongValue, LongValue>> value, Collector<Tuple2<T, T>> out)
                throws Exception {
            Tuple3<ET, LongValue, LongValue> degrees = value.f2;
            long sourceDegree = degrees.f1.getValue();
            long targetDegree = degrees.f2.getValue();

            if (sourceDegree < targetDegree ||
                    (sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
                edge.f0 = value.f0;
                edge.f1 = value.f1;
                out.collect(edge);
            }
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
            implements GroupReduceFunction<Tuple2<T, T>, Tuple3<T, T, T>> {
        private Tuple3<T, T, T> output = new Tuple3<>();

        private List<T> visited = new ArrayList<>();

        @Override
        public void reduce(Iterable<Tuple2<T, T>> values, Collector<Tuple3<T, T, T>> out)
                throws Exception {
            int visitedCount = 0;

            Iterator<Tuple2<T, T>> iter = values.iterator();

            while (true) {
                Tuple2<T, T> edge = iter.next();

                output.f0 = edge.f0;
                output.f2 = edge.f1;

                for (int i = 0; i < visitedCount; i++) {
                    output.f1 = visited.get(i);
                    out.collect(output);
                }

                if (! iter.hasNext()) {
                    break;
                }

                if (visitedCount == visited.size()) {
                    visited.add(edge.f1.copy());
                } else {
                    edge.f1.copyTo(visited.get(visitedCount));
                }

                visitedCount += 1;
            }
        }
    }

    /**
     * Simply project the triplet as a triangle.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0; 1; 2")
    @FunctionAnnotation.ForwardedFieldsSecond("0; 1")
    private static final class ProjectTriangles<T>
            implements JoinFunction<Tuple3<T, T, T>, Tuple2<T, T>, Tuple3<T, T, T>> {
        @Override
        public Tuple3<T, T, T> join(Tuple3<T, T, T> triplet, Tuple2<T, T> edge)
                throws Exception {
            return triplet;
        }
    }

    /**
     * Reorders the vertices of each emitted triangle (K0, K1, K2)
     * into sorted order such that K0 < K1 < K2.
     *
     * @param <T> ID type
     */
    private static final class SortTriangleVertices<T extends Comparable<T>>
            implements MapFunction<Tuple3<T, T, T>, Tuple3<T, T, T>> {
        @Override
        public Tuple3<T, T, T> map(Tuple3<T, T, T> value)
                throws Exception {
            // by the triangle listing algorithm we know f1 < f2
            if (value.f0.compareTo(value.f1) > 0) {
                T temp_val = value.f0;
                value.f0 = value.f1;

                if (temp_val.compareTo(value.f2) <= 0) {
                    value.f1 = temp_val;
                } else {
                    value.f1 = value.f2;
                    value.f2 = temp_val;
                }
            }

            return value;
        }
    }
}