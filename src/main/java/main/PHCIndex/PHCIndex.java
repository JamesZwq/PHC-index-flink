package main.PHCIndex;

import main.PHCIndex.CoreDecomposition.CoreDecomposition;
import main.PHCIndex.CoreTime.CTValue;
import main.PHCIndex.CoreTime.CoreTime;
import main.PHCIndex.PHC.PHC;
import main.PHCIndex.PHC.PHCValue;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.*;

public class PHCIndex<K extends Comparable<K>> implements GraphAlgorithm<K, NullValue, Integer, DataSet<Vertex<K, PHCValue<K>>>> {

    private final Integer maxIterations;

    public PHCIndex(int maxIterations) {
        this.maxIterations = maxIterations;
    }
    @Override
    public DataSet<Vertex<K, PHCValue<K>>> run(Graph<K, NullValue, Integer> input) throws Exception {
        DataSet<Vertex<K, Integer>> Cores = new CoreDecomposition<K, Integer>(maxIterations).run(input);
        Graph<K, Integer, Integer>result  = Graph.fromDataSet(Cores, input.getEdges(), input.getContext());
        CoreTime<K> kCoreTime = new CoreTime<>(maxIterations);
        DataSet<Vertex<K, ArrayList<Integer>>> CoreTimeR = kCoreTime.run(result);
        DataSet<Vertex<K, CTValue<K>>> vertices = kCoreTime.getVertices();
//        vertices.sortPartition(0, Order.ASCENDING).setParallelism(1).print();
        Graph<K, CTValue<K>, Integer> kcTvalueIntegerGraph = Graph.fromDataSet(vertices, input.getEdges(), input.getContext());
//        Cores.sortPartition(0, Order.ASCENDING).setParallelism(1).print();
        new PHC<K>(maxIterations).run(kcTvalueIntegerGraph);
        return null;
    }
}
