// I/O Efficient Core Graph Decomposition at Web Scale

package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class CoreDecomposition<K extends Comparable<K>, EV> implements GraphAlgorithm<K, NullValue, EV, DataSet<Vertex<K, Integer>>> {

    private final int maxIterations;

    public CoreDecomposition(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Integer>> run(Graph<K, NullValue, EV> input) throws Exception {
        DataSet<Vertex<K, LongValue>> degree = input.getUndirected().run(new Simplify<K, NullValue, EV>(false)).run(new VertexInDegree<K, NullValue, EV>());
        MapOperator<Vertex<K, LongValue>, Vertex<K, CDVertexValue<K>>> map = degree.map(new MapFunction<Vertex<K, LongValue>, Vertex<K, CDVertexValue<K>>>() {
            @Override
            public Vertex<K, CDVertexValue<K>> map(Vertex<K, LongValue> vertex) throws Exception {
                return new Vertex<>(vertex.getId(), new CDVertexValue<K>(Integer.parseInt(vertex.getValue().toString())));
            }
        });
        Graph<K, CDVertexValue<K>, EV> graph = Graph.fromDataSet(map, input.getEdges(), input.getContext());
        return graph.runScatterGatherIteration(new CDMessager<K, EV>(), new CDUpdater<K>(), maxIterations).mapVertices(new MapFunction<Vertex<K, CDVertexValue<K>>, Integer>() {
            @Override
            public Integer map(Vertex<K, CDVertexValue<K>> vertex) throws Exception {
                return vertex.getValue().getCore();
            }
        }).getVertices();
    }
}

