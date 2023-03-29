package main.PHCIndex.CoreTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class CoreTime<K extends Comparable<K>> implements GraphAlgorithm<K, Integer, Integer, DataSet<Vertex<K, ArrayList<Integer>>>> {

    private final int maxIterations;
    public static int maxTime;

    public CoreTime(int maxIterations) {
        this.maxIterations = maxIterations;
        maxTime = 0;
    }
    @Override
    public DataSet<Vertex<K, ArrayList<Integer>>> run(Graph<K, Integer, Integer> input) throws Exception {
        maxTime = input.getEdges().map(new MapFunction<Edge<K, Integer>, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer,Integer> map(Edge<K, Integer> edge) throws Exception {
                return new Tuple2<>(edge.getValue(), edge.getValue());
            }
        }).maxBy(0).collect().get(0).f1;

        MapOperator<Tuple2<Tuple2<K, ArrayList<NeighborValue<K>>>, Vertex<K, Integer>>, Vertex<K, CTvalue<K>>> vertex = input
                .getEdges()
                .groupBy(0, 1)
                .reduceGroup(new CTEdgeGroupReducer<>())
                .name("CoreTime: Edge Group Reducer to get the minimum time")
                .join(input.getVertices())
                .where(1)
                .equalTo(0)
                .name("CoreTime: Join with the vertices to get the core number")
                .groupBy(
                        new KeySelector<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>, K>() {
                            @Override
                            public K getKey(Tuple2<Edge<K, Integer>, Vertex<K, Integer>> value) throws Exception {
                                return value.f0.getSource();
                            }
                        }
                )
                .reduceGroup(new CTEdgeGroupReducerCoreTimeList<>())
                .name("CoreTime: Edge Group Reducer to get the core time list with neighbours")
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .name("CoreTime: Join with the vertices to get the core number of source")
                .map(new MapFunction<Tuple2<Tuple2<K, ArrayList<NeighborValue<K>>>, Vertex<K, Integer>>, Vertex<K, CTvalue<K>>>() {
                    @Override
                    public Vertex<K, CTvalue<K>> map(Tuple2<Tuple2<K, ArrayList<NeighborValue<K>>>, Vertex<K, Integer>> value) throws Exception {
                        return new Vertex<>(value.f0.f0, new CTvalue<>(value.f1.f1, value.f0.f1));
                    }
                });

        Graph<K, CTvalue<K>, Integer> graph = Graph.fromDataSet(vertex, input.getEdges(), input.getContext());
        return graph.runScatterGatherIteration(new CTMessager<K>(),
                        new CTUpdater<K>(),
                        maxIterations)
                .getVertices()
                .map(new MapFunction<Vertex<K, CTvalue<K>>, Vertex<K, ArrayList<Integer>>>() {
                    @Override
                    public Vertex<K, ArrayList<Integer>> map(Vertex<K, CTvalue<K>> value) throws Exception {
                        return new Vertex<>(value.getId(), value.getValue().getCoreTime());
                    }
                });
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

    private static class CTEdgeGroupReducerCoreTimeList<K extends Comparable<K>> implements GroupReduceFunction<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>, Tuple2<K, ArrayList<NeighborValue<K>>>> {
        @Override
        public void reduce(Iterable<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>> edges, Collector<Tuple2<K, ArrayList<NeighborValue<K>>>> out) {
            ArrayList<NeighborValue<K>> coreTimeList = new ArrayList<>();
            K vertex = null;
            for (Tuple2<Edge<K, Integer>, Vertex<K, Integer>> edge : edges) {
                coreTimeList.add(new NeighborValue<>(edge.f1.getId(), edge.f0.getValue(), edge.f1.getValue()));
                vertex = edge.f0.getSource();
            }
            coreTimeList.sort(new Comparator<NeighborValue<K>>() {
                @Override
                public int compare(NeighborValue<K> o1, NeighborValue<K> o2) {
                    return o1.getTime() - o2.getTime();
                }
            });
            assert vertex != null;
            out.collect(new Tuple2<>(vertex, coreTimeList));
        }

    }
}

