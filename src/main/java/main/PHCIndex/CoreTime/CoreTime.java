package main.PHCIndex.CoreTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class CoreTime<K extends Comparable<K>> implements GraphAlgorithm<K, Integer, Integer, DataSet<Vertex<K, ArrayList<Integer>>>> {

    private final int maxIterations;

    public CoreTime(int maxIterations) {
        this.maxIterations = maxIterations;
    }
    @Override
    public DataSet<Vertex<K, ArrayList<Integer>>> run(Graph<K, Integer, Integer> input) throws Exception {
        MapOperator<Tuple2<Tuple2<K, ArrayList<Tuple2<K,Integer>>>, Vertex<K, Integer>>, Vertex<K, CTvalue<K>>> vertex = input
                .getEdges()
                .groupBy(0, 1)
                .reduceGroup(new CTEdgeGroupReducer<>())
                .name("CoreTime: Edge Group Reducer to get the minimum time")
                .groupBy(0)
                .reduceGroup(new CTEdgeGroupReducerCoreTimeList<>())
                .name("CoreTime: Edge Group Reducer to get the core time list with neighbours")
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Tuple2<K, ArrayList<Tuple2<K,Integer>>>, Vertex<K, Integer>>, Vertex<K, CTvalue<K>>>() {
                    @Override
                    public Vertex<K, CTvalue<K>> map(Tuple2<Tuple2<K, ArrayList<Tuple2<K,Integer>>>, Vertex<K, Integer>> value) throws Exception {
                        return new Vertex<>(value.f0.f0, new CTvalue<>(value.f1.getValue(), value.f0.f1));
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

    private static class CTEdgeGroupReducerCoreTimeList<K extends Comparable<K>> implements GroupReduceFunction<Edge<K, Integer>, Tuple2<K,ArrayList<Tuple2<K,Integer>>>> {
        @Override
        public void reduce(Iterable<Edge<K, Integer>> values, Collector<Tuple2<K,ArrayList<Tuple2<K,Integer>>>> out) throws Exception {
            ArrayList<Tuple2<K,Integer>> timeList = new ArrayList<>();
            K key = null;
            for (Edge<K, Integer> edge : values) {
                key = edge.getSource();
                timeList.add(new Tuple2<>(edge.getTarget(), edge.getValue()));
            }
            timeList.sort((o1, o2) -> o1.f1.compareTo(o2.f1));
            out.collect(new Tuple2<>(key, timeList));
        }
    }
}

