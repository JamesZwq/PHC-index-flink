package main.PHCIndex.PHC;

import main.PHCIndex.CoreTime.CTValue;
import main.PHCIndex.CoreTime.NeighborValue;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

public class PHC<K extends Comparable<K>> implements GraphAlgorithm<K, CTValue<K>, Integer, DataSet<Vertex<K, ArrayList<Tuple2<Integer, Integer>>>>> {
    private final int maxIterations;

    public PHC(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, ArrayList<Tuple2<Integer, Integer>>>> run(Graph<K, CTValue<K>, Integer> input) throws Exception {
        int maxTime = input.getEdges().max(2).collect().get(0).f2;
        MapOperator<Tuple2<Vertex<K, HashMap<K, ArrayList<Integer>>>, Vertex<K, CTValue<K>>>, Vertex<K, PHCValue<K>>> vertex = input
                .getEdges()
                .groupBy(0)
                .reduceGroup(new PHCEdgeGroupReducer<>())
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .map(new PHCMapVertex<>(maxTime));
        Graph<K, PHCValue<K>, Integer> graph = Graph.fromDataSet(vertex, input.getEdges(), input.getContext());
//        graph.getVertices().print();
        SortPartitionOperator<Vertex<K, PHCValue<K>>> result = graph.runScatterGatherIteration(new PHCMessager<>(), new PHCUpdater<>(), maxIterations)
                .getVertices()
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1);
        result.print();
        return null;
    }

    private static class PHCEdgeGroupReducer<K extends Comparable<K>> implements GroupReduceFunction<Edge<K, Integer>, Vertex<K, HashMap<K, ArrayList<Integer>>>> {


        @Override
        public void reduce(Iterable<Edge<K, Integer>> values, Collector<Vertex<K, HashMap<K, ArrayList<Integer>>>> out) throws Exception {
            K source = null;
            HashMap<K, ArrayList<Integer>> neighborValues = new HashMap<>();
            for (Edge<K, Integer> edge : values) {
                source = edge.getSource();
                if (neighborValues.containsKey(edge.getTarget())) {
                    neighborValues.get(edge.getTarget()).add(edge.getValue());
                } else {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(edge.getValue());
                    neighborValues.put(edge.getTarget(), list);
                }
            }
            out.collect(new Vertex<>(source, neighborValues));
        }
    }

    private static class PHCMapVertex<K extends Comparable<K>> implements MapFunction<Tuple2<Vertex<K, HashMap<K, ArrayList<Integer>>>, Vertex<K, CTValue<K>>>, Vertex<K, PHCValue<K>>> {
        private final int maxTime;

        public PHCMapVertex(int maxTime) {
            this.maxTime = maxTime;
        }

        @Override
        public Vertex<K, PHCValue<K>> map(Tuple2<Vertex<K, HashMap<K, ArrayList<Integer>>>, Vertex<K, CTValue<K>>> value) throws Exception {
            K source = value.f0.getId();
            ArrayList<Tuple3<K, ArrayList<Integer>, ArrayList<Integer>>> neighborValues = new ArrayList<>();
            HashMap<K, ArrayList<Integer>> neighborEdgeTimes = value.f0.getValue();
            CTValue<K> coreTimes = value.f1.getValue();
            ArrayList<NeighborValue<K>> nebrTimeMap = coreTimes.getNebrTimeMap();
            for (NeighborValue<K> neighborValue : nebrTimeMap) {
                K neighbor = neighborValue.getKey();
//                neighborValues.put(neighbor, new PHCNeighborValue(neighborEdgeTimes.get(neighbor),neighborValue.getCoreTime()));
                neighborValues.add(new Tuple3<>(neighbor, neighborEdgeTimes.get(neighbor), neighborValue.getCoreTime()));
            }
            return new Vertex<>(source, new PHCValue<>(coreTimes.getCore(), neighborValues, coreTimes.getCoreTime(), maxTime));
        }
    }
}
