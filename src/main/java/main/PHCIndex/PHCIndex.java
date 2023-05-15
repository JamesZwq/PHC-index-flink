package main.PHCIndex;

import main.PHCIndex.CoreDecomposition.CoreDecomposition;
import main.PHCIndex.CoreDecomposition_base.CoreDecomposition_base;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

public class PHCIndex<K extends Comparable<K>> implements GraphAlgorithm<K, NullValue, Integer, DataSet<Vertex<K, Integer>>> {

    private final Integer maxIterations;

    public PHCIndex(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Integer>> run(Graph<K, NullValue, Integer> input) throws Exception {
//        DataSet<Vertex<K, Integer>> Cores = new CoreDecomposition_base<K, Integer>(maxIterations).run(input);
//        String path = "/Users/zhangwenqian/UNSW/tmp/cdb";
        DataSet<Vertex<K, Integer>> Cores = new CoreDecomposition<K, Integer>(maxIterations).run(input);
        String path = "/Users/zhangwenqian/UNSW/tmp/cda";
        Cores.sortPartition(0, org.apache.flink.api.common.operators.Order.ASCENDING).writeAsText(path, FileSystem.WriteMode.OVERWRITE);
        input.getContext().execute();
        return null;
    }
}
