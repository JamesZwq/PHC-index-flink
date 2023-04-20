package main.PHCIndex.CoreTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;

public class CoreTime<K extends Comparable<K>> implements GraphAlgorithm<K, Integer, Integer, DataSet<Vertex<K, ArrayList<Integer>>>> {
    public static int runTimes = 0;
    private final int maxIterations;
    DataSet<Vertex<K, CTValue<K>>> vertices;
//    private

    public CoreTime(int maxIterations) {
        this.maxIterations = maxIterations;
        vertices = null;
    }

    @Override
    public DataSet<Vertex<K, ArrayList<Integer>>> run(Graph<K, Integer, Integer> input) throws Exception {

        DataSet<Edge<K, Integer>> minEdgeTime = input
                .getEdges()
                .groupBy(0, 1)
                .reduceGroup(new CTEdgeGroupReducer<>());

        DataSet<Vertex<K, CTValue<K>>> map1 = minEdgeTime
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Edge<K, Integer>, Vertex<K, ArrayList<NeighborValue<K>>>>() {
                    @Override
                    public void reduce(Iterable<Edge<K, Integer>> values, Collector<Vertex<K, ArrayList<NeighborValue<K>>>> out) {
                        K source = null;
                        ArrayList<NeighborValue<K>> neighborValues = new ArrayList<>();
                        for (Edge<K, Integer> edge : values) {
                            source = edge.getSource();
                            neighborValues.add(new NeighborValue<>(edge.getTarget(), edge.getValue(), 0));
                        }
                        out.collect(new Vertex<>(source, neighborValues));
                    }
                })
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .name("CoreTime: Join with the vertices to get the core number")
                .map(new MapFunction<Tuple2<Vertex<K, ArrayList<NeighborValue<K>>>, Vertex<K, Integer>>, Vertex<K, CTValue<K>>>() {
                    @Override
                    public Vertex<K, CTValue<K>> map(Tuple2<Vertex<K, ArrayList<NeighborValue<K>>>, Vertex<K, Integer>> value) {
                        return new Vertex<>(value.f0.getId(), new CTValue<>(value.f1.getValue(), value.f0.getValue()));
                    }
                });
//        Tuple4<key, key, nbr, coreTIme>
        DataSet<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>> maped = minEdgeTime
                .join(input.getVertices())
                .where(1)
                .equalTo(0)
                .groupBy(new KeySelector<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>, K>() {
                    @Override
                    public K getKey(Tuple2<Edge<K, Integer>, Vertex<K, Integer>> value) throws Exception {
                        return value.f0.getSource();
                    }
                })
                .reduceGroup(new GroupReduceFunction<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>, Tuple2<K, ArrayList<Tuple3<K, Integer, Integer>>>>() {
                    //                    Tuple3<key, minTime, coreNumber>
                    @Override
                    public void reduce(Iterable<Tuple2<Edge<K, Integer>, Vertex<K, Integer>>> values, Collector<Tuple2<K, ArrayList<Tuple3<K, Integer, Integer>>>> out) throws Exception {
                        K source = null;
                        ArrayList<Tuple3<K, Integer, Integer>> result = new ArrayList<>();
                        for (Tuple2<Edge<K, Integer>, Vertex<K, Integer>> value : values) {
                            source = value.f0.getSource();
                            result.add(new Tuple3<>(value.f0.getTarget(), value.f0.getValue(), value.f1.getValue()));
                        }
                        out.collect(new Tuple2<>(source, result));
                    }
                })
                .join(input.getVertices())
                .where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Tuple2<K, ArrayList<Tuple3<K, Integer, Integer>>>, Vertex<K, Integer>>, Tuple3<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>>>() {
                    @Override
                    public Tuple3<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>> map(Tuple2<Tuple2<K, ArrayList<Tuple3<K, Integer, Integer>>>, Vertex<K, Integer>> value) throws Exception {
                        return new Tuple3<>(value.f0.f0, value.f1.getValue(), value.f0.f1);
                    }
                })
                .map(new MapFunction<Tuple3<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>>, Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>>() {
                    @Override
                    public Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>> map(Tuple3<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>> value) throws Exception {
                        System.out.println("maped: " + value);
                        ArrayList<Integer> coreTime = new ArrayList<>();
                        ArrayList<Tuple3<K, Integer, Integer>> nbr = value.f2.stream().sorted(Comparator.comparing(o -> o.f1)).collect(Collectors.toCollection(ArrayList::new));
//                        init coreTime
                        for (int i = 0; i < value.f1; i++) {
                            coreTime.add(nbr.get(i).f1);
                        }
                        return new Tuple4<>(value.f0, value.f1, nbr, coreTime);
                    }
                });
        DataSet<Tuple3<K, Integer, ArrayList<Integer>>> KKCT = maped.map(new MapFunction<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>, Tuple3<K, Integer, ArrayList<Integer>>>() {
            @Override
            public Tuple3<K, Integer, ArrayList<Integer>> map(Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, value.f3);
            }
        });
        DataSet<Vertex<K, CTValue<K>>> map2 = minEdgeTime
                .join(KKCT)
                .where(1)
                .equalTo(0)
                .join(maped)
                .where(new KeySelector<Tuple2<Edge<K, Integer>, Tuple3<K, Integer, ArrayList<Integer>>>, K>() {

                    @Override
                    public K getKey(Tuple2<Edge<K, Integer>, Tuple3<K, Integer, ArrayList<Integer>>> value) throws Exception {
                        return value.f0.getSource();
                    }
                })
                .equalTo(0)
                .map(new MapFunction<
                        Tuple2<
                                Tuple2<Edge<K, Integer>,
                                        Tuple3<K, Integer, ArrayList<Integer>>>,
                                Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>>, Tuple2<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>, Tuple3<K, Integer, ArrayList<Integer>>>>() {
                    @Override
                    public Tuple2<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>, Tuple3<K, Integer, ArrayList<Integer>>> map(Tuple2<Tuple2<Edge<K, Integer>, Tuple3<K, Integer, ArrayList<Integer>>>, Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>> value) throws Exception {
                        return new Tuple2<>(value.f1, value.f0.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>, Tuple3<K, Integer, ArrayList<Integer>>>,
                        Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>>>() {
                    @Override
                    public Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>> map(Tuple2<Tuple4<K, Integer, ArrayList<Tuple3<K, Integer, Integer>>, ArrayList<Integer>>, Tuple3<K, Integer, ArrayList<Integer>>> value) throws Exception {
                        ArrayList<Tuple3<K, Integer, Integer>> minTimes = value.f0.f2;
                        int minTime = -1;
                        for (Tuple3<K, Integer, Integer> minTime1 : minTimes) {
                            if (minTime1.f0.equals(value.f1.f0)) {
                                minTime = minTime1.f1;
                                break;
                            }
                        }
                        return new Tuple2<>(new Tuple3<>(value.f0.f0, value.f0.f1, value.f0.f3), new Tuple4<>(value.f1.f0, value.f1.f1, minTime, value.f1.f2));
                    }
                })
                .groupBy(new KeySelector<Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>>, K>() {
                    @Override
                    public K getKey(Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>> value) throws Exception {
                        return value.f0.f0;
                    }
                })
                .reduceGroup(new GroupReduceFunction<Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>>, Vertex<K, CTValue<K>>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>>> values, Collector<Vertex<K, CTValue<K>>> out) throws Exception {
                        Tuple3<K, Integer, ArrayList<Integer>> key = null;
                        ArrayList<NeighborValue<K>> neighbors = new ArrayList<>();
//                         public NeighborValue(K key, Integer time, Integer core, ArrayList<Integer> coreTime) {
                        for (Tuple2<Tuple3<K, Integer, ArrayList<Integer>>, Tuple4<K, Integer, Integer, ArrayList<Integer>>> value : values) {
                            if (key == null) {
                                key = value.f0;
                            }
                            neighbors.add(new NeighborValue<>(value.f1.f0, value.f1.f2, value.f1.f1, value.f1.f3));
//                            neighbors.add(new NeighborValue<>(value.f1.f0, value.f1.f2, value.f1.f1));
                        }
                        out.collect(new Vertex<>(key.f0, new CTValue<>(key.f1, key.f2, neighbors)));
//                        out.collect(new Vertex<>(key.f0, new CTValue<>(key.f1, neighbors)));
                    }
                });

        Graph<K, CTValue<K>, Integer> graph = Graph.fromDataSet(map1, input.getEdges(), input.getContext());
        DataSet<Vertex<K, CTValue<K>>> result = graph.runScatterGatherIteration(new CTMessager<>(),
                        new CTUpdater<>(),
                        maxIterations)
                .getVertices();
        this.vertices = result;
        result.sortPartition(0, Order.ASCENDING).setParallelism(1).writeAsText("/Users/zhangwenqian/UNSW/tmp/a", FileSystem.WriteMode.OVERWRITE);
        result.getExecutionEnvironment().execute();
        return result
                .map(new MapFunction<Vertex<K, CTValue<K>>, Vertex<K, ArrayList<Integer>>>() {
                    @Override
                    public Vertex<K, ArrayList<Integer>> map(Vertex<K, CTValue<K>> value) {
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

