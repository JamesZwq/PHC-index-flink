package main.PHCIndex;

import main.IOEfficientCore.I_O_efficient_Core;
import main.IOEfficientCore.MyVertex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;

import java.util.*;
import java.util.stream.Collectors;

public class PHCIndex<K> implements GraphAlgorithm<K, NullValue, Integer, DataSet<Vertex<K, VertexValue<K>>>> {

    private final Integer maxIterations;

    public PHCIndex(int maxIterations) {
        this.maxIterations = maxIterations;
    }
    @Override
    public DataSet<Vertex<K, VertexValue<K>>> run(Graph<K, NullValue, Integer> input) throws Exception {
//        get core of each vertex for all graph
//        cover to HashMap<K, Integer> core
        HashMap<K, Integer> core = new I_O_efficient_Core<K, Integer>(maxIterations).run(input).collect().stream().collect(HashMap::new, (m, v) -> m.put(v.getId(), v.getValue().getCore()), HashMap::putAll);
        List<Edge<K,Integer>> allEdge = input.getEdges().collect();
        return input.mapVertices(new InitVerticesMapper<>(core, allEdge)).runScatterGatherIteration(new InitCT.InitCTMessenger(), new InitCT.InitCTUpdater(), maxIterations).getVertices();
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
            HashMap<K,Integer> CN = new HashMap<>();

            for (Edge<K,Integer> e : edgeAtS) {
                if (e.getSource().equals(value.getId()) || e.getTarget().equals(value.getId())) {
                    K target = e.getTarget().equals(value.getId()) ? e.getSource() : e.getTarget();
                    if (core.get(target) < core.get(value.getId())) continue;
                    if (!CN.containsKey(target)) {
                        CN.put(target, 1);
                    } else {
                        CN.put(target, CN.get(target) + 1);
                    }
                }
            }

            for (K k : CN.keySet()) {
                nei.put(k, new NeighborsValue(core.get(k), CN.get(k)));
            }

            return new VertexValue<>(core.get(value.getId()), nei);
        }
    }

    private static final class ComputeHelper<K> {
        public HashMap<K,Integer> Compute_CN(K u, HashMap<K,Integer> neighbors, HashMap<K, Integer> core) {
            HashMap<K,Integer> CN = new HashMap<>();
            for (K edge : neighbors.keySet()) {
                K v = edge;
                if (core.get(v) < core.get(u)) continue;
                if (!CN.containsKey(u)) {
                    CN.put(u, 1);
                } else {
                    CN.put(u, CN.get(u) + 1);
                }
            }
            return CN;
        }
    }

    private static final class InitCT<K> {
        public static final class InitCTMessenger<K>
                extends ScatterFunction<K, VertexValue<K>, PHCMessage, NullValue> {

            @Override
            public void sendMessages(Vertex<K, VertexValue<K>> vertex) throws Exception {

            }
        }

        public static final class InitCTUpdater<K> extends GatherFunction<K, VertexValue<K>, PHCMessage> {

            @Override
            public void updateVertex(Vertex<K, VertexValue<K>> vertex, MessageIterator<PHCMessage> inMessages) throws Exception {

            }
        }

    }

}
