package main.PHCIndex.CoreTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class CoreTime<K extends Comparable<K>> implements GraphAlgorithm<K, Integer, Integer, DataSet<Vertex<K, ArrayList<Integer>>>> {
    private final int maxIterations;
    DataSet<Vertex<K, CTValue<K>>> vertices;
//    private

    public CoreTime(int maxIterations) {
        this.maxIterations = maxIterations;
        vertices = null;
    }

    @Override
    public DataSet<Vertex<K, ArrayList<Integer>>> run(Graph<K, Integer, Integer> input) throws Exception {

        MapOperator<Tuple2<Vertex<K, CTValue<K>>, Vertex<K, Integer>>, Vertex<K, CTValue<K>>> map = input
                .getEdges()
                .groupBy(0, 1)
                .reduceGroup(new CTEdgeGroupReducer<>())
                .name("CoreTime: Edge Group Reducer to get the minimum time")
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Edge<K, Integer>, Vertex<K, CTValue<K>>>() {
                    @Override
                    public void reduce(Iterable<Edge<K, Integer>> values, Collector<Vertex<K, CTValue<K>>> out) {
                        K source = null;
                        ArrayList<NeighborValue<K>> neighborValues = new ArrayList<>();
                        for (Edge<K, Integer> edge : values) {
                            source = edge.getSource();
                            neighborValues.add(new NeighborValue<>(edge.getTarget(), edge.getValue(), 0));
                        }
                        out.collect(new Vertex<>(source, new CTValue<>(0, neighborValues)));
                    }
                })
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .name("CoreTime: Join with the vertices to get the core number")
                .map(new MapFunction<Tuple2<Vertex<K, CTValue<K>>, Vertex<K, Integer>>, Vertex<K, CTValue<K>>>() {
                    @Override
                    public Vertex<K, CTValue<K>> map(Tuple2<Vertex<K, CTValue<K>>, Vertex<K, Integer>> value) {
                        CTValue<K> value1 = value.f0.getValue();
                        value1.setCore(value.f1.getValue());
                        return new Vertex<>(value.f0.getId(), value1);
                    }
                });
        Graph<K, CTValue<K>, Integer> graph = Graph.fromDataSet(map, input.getEdges(), input.getContext());
        DataSet<Vertex<K, CTValue<K>>> result = graph.runScatterGatherIteration(new CTMessager<>(),
                        new CTUpdater<>(),
                        maxIterations)
                .getVertices();
        this.vertices = result;
        return result
                .map(new MapFunction<Vertex<K, CTValue<K>>, Vertex<K, ArrayList<Integer>>>() {
                    @Override
                    public Vertex<K, ArrayList<Integer>> map(Vertex<K, CTValue<K>> value) {
                        System.out.println(value);
                        return new Vertex<>(value.getId(), value.getValue().getCoreTime());
                    }
                });
    }

    public DataSet<Vertex<K, CTValue<K>>> getVertices() {
        return vertices;
    }

    private static class CTEdgeGroupReducer<K extends Comparable<K>, EV extends Comparable<EV>> implements GroupReduceFunction<Edge<K, EV>, Edge<K, EV>> {
        @Override
        public void reduce(Iterable<Edge<K, EV>> edges, Collector<Edge<K, EV>> out) {
            EV minTime = null;
            K source = null;
            K target = null;
            for (Edge<K, EV> edge : edges) {
                EV value = edge.getValue();
                source = edge.getSource();
                target = edge.getTarget();
                if (minTime == null || value.compareTo(minTime) < 0) {
                    minTime = value;
                }
            }
            out.collect(new Edge<>(source, target, minTime));
        }
    }
}

