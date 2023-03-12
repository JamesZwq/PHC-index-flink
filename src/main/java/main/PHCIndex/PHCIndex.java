package main.PHCIndex;

import main.IOEfficientCore.I_O_efficient_Core;
import main.PHCIndex.InitCT.InitCTMessenger;
import main.PHCIndex.InitCT.InitCTUpdater;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;

import java.util.*;

public class PHCIndex<K> implements GraphAlgorithm<K, NullValue, Integer, DataSet<Vertex<K, VertexValue<K>>>> {

    private final Integer maxIterations;

    public PHCIndex(int maxIterations) {
        this.maxIterations = maxIterations;
    }
    @Override
    public DataSet<Vertex<K, VertexValue<K>>> run(Graph<K, NullValue, Integer> input) throws Exception {
        HashMap<K, Integer> core = new I_O_efficient_Core<K, Integer>(maxIterations).run(input).collect().stream().collect(HashMap::new, (m, v) -> m.put(v.getId(), v.getValue().getCore()), HashMap::putAll);
        List<Edge<K,Integer>> allEdge = input.getEdges().collect();
        List<Integer> timeStamps = input.getEdges().map(new MapFunction<Edge<K, Integer>, Integer>() {
            @Override
            public Integer map(Edge<K, Integer> edge) throws Exception {
                return edge.getValue();
            }
        }).distinct().collect();
        timeStamps.sort(Comparator.reverseOrder());
        Graph<K, VertexValue<K>, Integer> result = input.mapVertices(new InitVerticesMapper<>(core, allEdge));
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.registerAggregator("maxIterations", new LongSumAggregator());
        MapFunction<Vertex<K, VertexValue<K>>, VertexValue<K>> mapFunction = new MapFunction<Vertex<K, VertexValue<K>>, VertexValue<K>>() {
            @Override
            public VertexValue<K> map(Vertex<K, VertexValue<K>> vertex) throws Exception {
                vertex.getValue().setCalculatedCoreCNFalse();
                return vertex.getValue();
            }
        };
        for (int t : timeStamps) {
            result = result
                    .runScatterGatherIteration(new InitCTMessenger<>(t), new InitCTUpdater<>(t), maxIterations, parameters)
                    .mapVertices(mapFunction);
        }
//        for (int t : timeStamps) {
//            result = result
//                    .runScatterGatherIteration(new PHCMessenger<>(t), new PHCUpdater<>(t), maxIterations, parameters)
//                    .mapVertices(mapFunction);
//        }
        return result.getVertices();
    }

    /**
     * @param <K> Type of the vertex Key.
     * @param <VV> Type of the TimeStamp.
     */
    private static final class InitVerticesMapper<K, VV>
            implements MapFunction<Vertex<K, VV>, VertexValue<K>> {

        private final HashMap<K, Integer> core;
        private final List<Edge<K,Integer>> edgeAtS;

        public InitVerticesMapper(HashMap<K, Integer> tmp_core, List<Edge<K,Integer>> edgeAtS) {
            this.core = tmp_core;
            this.edgeAtS = edgeAtS;
        }

        public VertexValue<K> map(Vertex<K, VV> value) {
            HashMap<K,NeighborsValue> nei = new HashMap<>();
            HashMap<K, Tuple2<Integer,List<Integer>>> CN = new HashMap<>();
            for (Edge<K,Integer> e : edgeAtS) {
                if (e.getSource().equals(value.getId())) {
                    K target = e.getTarget();
//                    record degree of each vertex
                    if (!CN.containsKey(target)) {
                        ArrayList<Integer> tmp = new ArrayList<>();
                        tmp.add(e.getValue());
                        CN.put(target, new Tuple2<>(0, tmp));
                    } else {
                        CN.get(target).f1.add(e.getValue());
                    }

                    if (core.get(target) < core.get(value.getId())) continue;
                    Tuple2<Integer,List<Integer>> tmp = CN.get(target);
                    tmp.f0 += 1;
                    CN.put(target, tmp);
                }
            }

            for (K k : CN.keySet()) {
                nei.put(k, new NeighborsValue(core.get(k), CN.get(k).f0, CN.get(k).f1));
            }

            return new VertexValue<>(core.get(value.getId()), nei);
        }
    }

}
