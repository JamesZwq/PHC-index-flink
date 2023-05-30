// I/O Efficient Core Graph Decomposition at Web Scale

package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class CoreDecomposition<K extends Comparable<K>, EV> implements GraphAlgorithm<K, NullValue, EV, DataSet<Vertex<K, Tuple3<Integer,Integer,Integer>>>> {

    private final int maxIterations;

    public CoreDecomposition(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Tuple3<Integer,Integer,Integer>>> run(Graph<K, NullValue, EV> input) throws Exception {
        Graph<K, NullValue, EV> evGraph = input.run(new Simplify<>(false));
        DataSet<Vertex<K, LongValue>> degree = evGraph.run(new VertexInDegree<>());
        DataSet<Vertex<K, CDVertexValue<K>>> map1 = evGraph
                .getEdges()
                .join(degree)
                .where(1)
                .equalTo(0)
                .groupBy(new KeySelector<Tuple2<Edge<K, EV>, Vertex<K, LongValue>>, K>() {
                    //                    group by source
                    @Override
                    public K getKey(Tuple2<Edge<K, EV>, Vertex<K, LongValue>> value) throws Exception {
                        return value.f0.getSource();
                    }
                })
                .reduceGroup(new GroupReduceFunction<Tuple2<Edge<K, EV>, Vertex<K, LongValue>>, Vertex<K, Tuple2<Integer, HashMap<K,Integer>>>>() {
                    //                    set the neighbor's degree
                    // 如果一个点没有任何的邻居，则删除这个点
                    @Override
                    public void reduce(Iterable<Tuple2<Edge<K, EV>, Vertex<K, LongValue>>> values, Collector<Vertex<K, Tuple2<Integer,HashMap<K,Integer>>>> out) throws Exception {
                        HashMap<K, Integer> map = new HashMap<>();
                        int core = Integer.MAX_VALUE;
                        K source = null;
                        for (Tuple2<Edge<K, EV>, Vertex<K, LongValue>> value : values) {
                            source = value.f0.getSource();
                            Vertex<K, LongValue> v = value.f1;
                            map.put(v.getId(), Integer.parseInt(v.getValue().toString()));
                        }
                        out.collect(new Vertex<>(source, new Tuple2<>(core, map)));
                    }
                })
                .join(degree)
                .where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Vertex<K, Tuple2<Integer, HashMap<K,Integer>>>, Vertex<K, LongValue>>, Vertex<K, CDVertexValue<K>>>() {
                    //                    set the source's degree
                    @Override
                    public Vertex<K, CDVertexValue<K>> map(Tuple2<Vertex<K, Tuple2<Integer, HashMap<K,Integer>>>, Vertex<K, LongValue>> value) throws Exception {
                        return new Vertex<>(value.f0.getId(), new CDVertexValue<>(Integer.parseInt(value.f1.getValue().toString()), value.f0.getValue().f1));
                    }
                });
        Graph<K, CDVertexValue<K>, EV> graph = Graph.fromDataSet(map1, evGraph.getEdges(), input.getContext());
        Graph<K, CDVertexValue<K>, EV> kcdVertexValueEVGraph = graph.runScatterGatherIteration(new CDMessager<K, EV>(), new CDUpdater<K>(), maxIterations);
        return kcdVertexValueEVGraph.mapVertices(new MapFunction<Vertex<K, CDVertexValue<K>>, Tuple3<Integer,Integer,Integer>>() {
            @Override
            public Tuple3<Integer,Integer,Integer> map(Vertex<K, CDVertexValue<K>> vertex) {
                return new Tuple3<>(vertex.getValue().getCore(), vertex.getValue().numMsg,vertex.getValue().completeAt);
            }
        }).getVertices();
    }
}

