package main.PHCIndex;

import main.IOEfficientCore.I_O_efficient_Core;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
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
//        get core of each vertex for all graph
//        cover to HashMap<K, Integer> core
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
        for (int t : timeStamps) {
//            if (t == timeStamps.get(timeStamps.size() - 1)) {
//                break;
//            }
            result = result
                    .runScatterGatherIteration(new InitCT.InitCTMessenger<>(t), new InitCT.InitCTUpdater<>(t), maxIterations, parameters)
                    .mapVertices(new MapFunction<Vertex<K, VertexValue<K>>, VertexValue<K>>() {
                        @Override
                        public VertexValue<K> map(Vertex<K, VertexValue<K>> vertex) throws Exception {
                            vertex.getValue().setCalculatedCoreCNFalse();
                            return vertex.getValue();
                        }
                    });
            result.getVertices().print();
        }
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

    private static final class ComputeHelper<K> {
        private int timeEnd;

        public ComputeHelper(int timeEnd) {
            this.timeEnd = timeEnd;
        }

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

    public static class InitCNMessage<K> {
        int core;
        K u;

        public InitCNMessage(K u, int core) {
            this.core = core;
            this.u = u;
        }

        public int getCore() {
            return core;
        }

        public K getSource() {
            return u;
        }
    }


    private static final class InitCT<K> {
        public static final class InitCTMessenger<K>
                extends ScatterFunction<K, VertexValue<K>, InitCNMessage<K>, Integer> {
            private int timeEnd;

            public InitCTMessenger(int timeStart) {
                this.timeEnd = timeStart;
            }

            @Override
            public void preSuperstep() throws Exception {
                super.preSuperstep();
            }

            @Override
            public void sendMessages(Vertex<K, VertexValue<K>> vertex) throws Exception {
                HashSet<K> visited = new HashSet<>();
                if(!vertex.getValue().isCalculated()) {
                    for (Edge<K, Integer> e : getEdges()) {
                        if (e.getValue() > timeEnd) continue;
                        if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                        if (visited.contains(e.getTarget())) continue;
                        NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                        if(!neighborsValue.decreaseCTN()){
                            sendMessageTo(e.getTarget(), new InitCNMessage<>(vertex.getId(), vertex.getValue().getCore()));
                            visited.add(e.getTarget());
                        }
                    }
                } else {
                    for (Edge<K, Integer> e : getEdges()) {
                        if (e.getValue() > timeEnd) continue;
                        if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                        if (visited.contains(e.getTarget())) continue;
                        NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                        if (vertex.getValue().getCore() < neighborsValue.getCore() && neighborsValue.getCore() <= vertex.getValue().getOldCore()) {
                            sendMessageTo(e.getTarget(), new InitCNMessage<>(vertex.getId(), vertex.getValue().getCore()));
                            visited.add(e.getTarget());
                        }
                    }
                }
            }
        }

        public static final class InitCTUpdater<K> extends GatherFunction<K, VertexValue<K>, InitCNMessage<K>> {

            private int timeEnd;

            public InitCTUpdater(int timeEnd) {
                this.timeEnd = timeEnd;
            }

            @Override
            public void preSuperstep() throws Exception {
                super.preSuperstep();
            }

            @Override
            public void updateVertex(Vertex<K, VertexValue<K>> vertex, MessageIterator<InitCNMessage<K>> inMessages) throws Exception {
                VertexValue<K> v = new VertexValue<>(vertex.getValue());

                boolean noUpdate = true;
                for (InitCNMessage<K> msg : inMessages) {
                    v.getNeighbors().get(msg.getSource()).setCore(msg.getCore());
                    if(v.isCalculated()){
                        v.getNeighbors().get(msg.getSource()).setCTNtoZero();
                        if(v.getCTNSize() < v.getCore()){
                            noUpdate = false;
                        }
                    } else {
                        noUpdate = false;
                    }
                }
                if (noUpdate) {
                    setNewVertexValue(v);
                    return;
                }
                v.setCalculatedCoreCN();
                int oldCore = v.getCore();
                v.setOldCore(oldCore);

                List<Integer> cnt = new ArrayList<>();
                for (int i = 0; i <= oldCore; ++i) {
                    cnt.add(0);
                }

                HashMap<K, NeighborsValue> neighbors = v.getNeighbors();
                for (K nei : neighbors.keySet()) {
                    if(neighbors.get(nei).getCoreTimeNb().stream().noneMatch(x -> x <= timeEnd)) continue;
                    int coreNei = neighbors.get(nei).getCore();
                    if (coreNei < oldCore) cnt.set(coreNei, cnt.get(coreNei) + 1);
                    else cnt.set(oldCore, cnt.get(oldCore) + 1);
                }
                int cd = 0;
                for (int k = oldCore; k >= 0 ; --k) {
                    cd += cnt.get(k);
                    if(cd >= k){
                        v.setCore(k);
                        break;
                    }
                }

                v.resetCoreTimeNeighbors();
                for (K nei : neighbors.keySet()) {
                    if(neighbors.get(nei).getCoreTimeNb().stream().noneMatch(x -> x <= timeEnd)) continue;
                    int coreNei = neighbors.get(nei).getCore();
                    if (coreNei < v.getCore()) continue;
                    v.getNeighbors().get(nei).increaseCTN();
                }

                for(int tmp_k = oldCore; tmp_k >= v.getCore(); --tmp_k){
                    v.addCoreTime(tmp_k, 1, timeEnd);
                }

                if(!v.equals(vertex.getValue())) setNewVertexValue(v);
            }
        }
    }

}
