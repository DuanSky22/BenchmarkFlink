package com.duansky.benchmark.flink.analysis.graph.library;

import com.duansky.benchmark.flink.analysis.util.Graphs;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;
import org.apache.flink.graph.utils.Murmur3_32;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * Created by DuanSky on 2016/11/21.
 */
public class _LocalClusteringCofficient {

    public static void main(String[] args) throws Exception {

        Graph<IntValue,NullValue,NullValue> input = Graphs.createGraph();
        test(input);

    }

    public static void test(Graph<IntValue, NullValue, NullValue> input) throws Exception {
        input.run(new LocalClusteringCoefficient<>())
        .collect()
        .forEach(r -> System.out.println(r.toVerboseString()))
        ;
    }

    public static void analysis(Graph<IntValue, NullValue, NullValue> input) throws Exception {

        // u, v, w
        DataSet<Tuple3<IntValue,IntValue,IntValue>> triangles = input
                .run(new TriangleListing<IntValue,NullValue,NullValue>());
        triangles.print();

        // u, 1
        DataSet<Tuple2<IntValue, LongValue>> triangleVertices = triangles
                .flatMap(new SplitTriangles<IntValue>())
                .name("Split triangle vertices");
        triangleVertices.print();

        // u, triangle count
        DataSet<Tuple2<IntValue, LongValue>> vertexTriangleCount = triangleVertices
                .groupBy(0)
                .reduce(new CountTriangles<IntValue>())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .name("Count triangles");
        vertexTriangleCount.print();

        // u, deg(u)
        DataSet<Vertex<IntValue, LongValue>> vertexDegree = input
                .run(new VertexDegree<IntValue, NullValue, NullValue>()
                        .setIncludeZeroDegreeVertices(true));
        vertexDegree.print();

        // u, deg(u), triangle count
        vertexDegree
                .leftOuterJoin(vertexTriangleCount)
                .where(0)
                .equalTo(0)
                .with(new JoinVertexDegreeWithTriangleCount<IntValue>())
                .name("Clustering coefficient")
                .collect()
                .forEach(r -> System.out.println(r));
    }

    /**
     * Emits the three vertex IDs comprising each triangle along with an initial count.
     *
     * @param <T> ID type
     */
    private static class SplitTriangles<T>
            implements FlatMapFunction<Tuple3<T, T, T>, Tuple2<T, LongValue>> {
        private Tuple2<T, LongValue> output = new Tuple2<>(null, new LongValue(1));

        @Override
        public void flatMap(Tuple3<T, T, T> value, Collector<Tuple2<T, LongValue>> out)
                throws Exception {
            output.f0 = value.f0;
            out.collect(output);

            output.f0 = value.f1;
            out.collect(output);

            output.f0 = value.f2;
            out.collect(output);
        }
    }

    /**
     * Sums the triangle count for each vertex ID.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("0")
    private static class CountTriangles<T>
            implements ReduceFunction<Tuple2<T, LongValue>> {
        @Override
        public Tuple2<T, LongValue> reduce(Tuple2<T, LongValue> left, Tuple2<T, LongValue> right)
                throws Exception {
            left.f1.setValue(left.f1.getValue() + right.f1.getValue());
            return left;
        }
    }

    /**
     * Joins the vertex and degree with the vertex's triangle count.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0; 1->1.0")
    @FunctionAnnotation.ForwardedFieldsSecond("0")
    private static class JoinVertexDegreeWithTriangleCount<T>
            implements JoinFunction<Vertex<T, LongValue>, Tuple2<T, LongValue>, LocalClusteringCoefficient.Result<T>> {
        private LongValue zero = new LongValue(0);

        private LocalClusteringCoefficient.Result<T> output = new LocalClusteringCoefficient.Result<>();

        @Override
        public LocalClusteringCoefficient.Result<T> join(Vertex<T, LongValue> vertexAndDegree, Tuple2<T, LongValue> vertexAndTriangleCount)
                throws Exception {
            output.f0 = vertexAndDegree.f0;
            output.f1.f0 = vertexAndDegree.f1;
            output.f1.f1 = (vertexAndTriangleCount == null) ? zero : vertexAndTriangleCount.f1;

            return output;
        }
    }

    /**
     * Wraps the vertex type to encapsulate results from the Local Clustering Coefficient algorithm.
     *
     * @param <T> ID type
     */
    public static class Result<T>
            extends Vertex<T, Tuple2<LongValue, LongValue>> {
        private static final int HASH_SEED = 0xc23937c1;

        private Murmur3_32 hasher = new Murmur3_32(HASH_SEED);

        public Result() {
            f1 = new Tuple2<>();
        }

        /**
         * Get the vertex degree.
         *
         * @return vertex degree
         */
        public LongValue getDegree() {
            return f1.f0;
        }

        /**
         * Get the number of triangles containing this vertex; equivalently,
         * this is the number of edges.txt between neighbors of this vertex.
         *
         * @return triangle count
         */
        public LongValue getTriangleCount() {
            return f1.f1;
        }

        /**
         * Get the local clustering coefficient score. This is computed as the
         * number of edges.txt between neighbors, equal to the triangle count,
         * divided by the number of potential edges.txt between neighbors.
         *
         * A score of {@code Double.NaN} is returned for a vertex with degree 1
         * for which both the triangle count and number of neighbors are zero.
         *
         * @return local clustering coefficient score
         */
        public double getLocalClusteringCoefficientScore() {
            long degree = getDegree().getValue();
            long neighborPairs = degree * (degree - 1) / 2;

            return (neighborPairs == 0) ? Double.NaN : getTriangleCount().getValue() / (double)neighborPairs;
        }

        /**
         * Format values into a human-readable string.
         *
         * @return verbose string
         */
        public String toVerboseString() {
            return "Vertex ID: " + f0
                    + ", vertex degree: " + getDegree()
                    + ", triangle count: " + getTriangleCount()
                    + ", local clustering coefficient: " + getLocalClusteringCoefficientScore();
        }

        @Override
        public int hashCode() {
            return hasher.reset()
                    .hash(f0.hashCode())
                    .hash(f1.f0.getValue())
                    .hash(f1.f1.getValue())
                    .hash();
        }
    }
}
