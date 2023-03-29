package main.PHCIndex;

import main.PHCIndex.CoreDecomposition.CoreDecomposition;
import main.PHCIndex.CoreTime.CoreTime;
import main.PHCIndex.PHCVertex.NeighborsValue;
import main.PHCIndex.PHCVertex.VertexValue;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.*;

public class PHCIndex<K extends Comparable<K>> implements GraphAlgorithm<K, NullValue, Integer, DataSet<Vertex<K, VertexValue<K>>>> {

    private final Integer maxIterations;

    public PHCIndex(int maxIterations) {
        this.maxIterations = maxIterations;
    }
    @Override
    public DataSet<Vertex<K, VertexValue<K>>> run(Graph<K, NullValue, Integer> input) throws Exception {
        DataSet<Vertex<K, Integer>> Cores = new CoreDecomposition<K, Integer>(maxIterations).run(input);
        Graph<K, Integer, Integer> result = Graph.fromDataSet(Cores, input.getEdges(), input.getContext());
//        new CoreTime<K>(maxIterations).run(result).sortPartition(0, Order.ASCENDING).print();
        Cores.sortPartition(0, Order.ASCENDING).writeAsText("/Users/zhangwenqian/UNSW/tmp/b", FileSystem.WriteMode.OVERWRITE);
        input.getContext().execute();
        return null;
    }

    /**
     * @param <K> Type of the vertex Key.
     * @param <VV> Type of the TimeStamp.
     */
    private static final class InitVerticesMapper<K, VV>
            implements MapFunction<Vertex<K, VV>, VertexValue<K>> {

        private HashMap<K, Integer> core;
        private List<Edge<K,Integer>> edgeAtS;


        public InitVerticesMapper(HashMap<K, Integer> tmp_core, List<Edge<K,Integer>> edgeAtS) {
            this.core = tmp_core;
            this.edgeAtS = edgeAtS;
        }

        public VertexValue<K> map(Vertex<K, VV> value) {
            HashMap<K, NeighborsValue> nei = new HashMap<>();
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
//                nei.put(k, new NeighborsValue(core.get(k), CN.get(k).f0, CN.get(k).f1));
            }

            return new VertexValue<>(core.get(value.getId()), nei);
        }
    }

}
