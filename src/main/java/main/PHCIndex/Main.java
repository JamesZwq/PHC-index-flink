package main.PHCIndex;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import main.PHCIndex.CoreDecomposition.CoreDecomposition;
import main.PHCIndex.CoreDecomposition_base.CoreDecomposition_base;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import javax.xml.crypto.KeySelector;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.out.println("Usage: <input_path> <log_path> <algorithm, 1 for base, 2 for total>");
            System.exit(1);
        }
//        Configuration conf = new Configuration();
//        conf.setString("taskmanager.memory.network.fraction", "0.2");
//        conf.setString("taskmanager.memory.network.min", "64mb");
//        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
//        env.setParallelism(1);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, NullValue>> edges = env.readTextFile(args[0]).flatMap(new readGraph()).distinct();
        Graph<Integer, NullValue, NullValue> graph = Graph.fromTupleDataSet(edges, env);
        DataSet<Vertex<Integer, Tuple3<Integer,Integer,Integer>>> Cores;
        if (Integer.parseInt(args[2]) == 1) {
            System.out.println("Using base algorithm");
            Cores = new CoreDecomposition_base<Integer, NullValue>(100000000).run(graph);
        } else {
            System.out.println("Using total algorithm");
            Cores = new CoreDecomposition<Integer, NullValue>(100000000).run(graph);
        }
//        Cores.map(new MapFunction<Vertex<Integer, Tuple3<Integer, Integer, Integer>>, Tuple4<Integer,Integer,Integer,Integer>>() {
//            @Override
//            public Tuple4<Integer, Integer, Integer, Integer> map(Vertex<Integer, Tuple3<Integer, Integer, Integer>> value) throws Exception {
//                return new Tuple4<>(value.getId(), value.getValue().f0, value.getValue().f1, value.getValue().f2);
//            }
//        }).sortPartition(0,Order.ASCENDING).print();

//        Cores.map(new MapFunction<Vertex<Integer, Tuple3<Integer, Integer, Integer>>, Tuple1<Integer>>() {
//            @Override
//            public Tuple1<Integer> map(Vertex<Integer, Tuple3<Integer, Integer, Integer>> value) throws Exception {
//                return new Tuple1<>(value.getValue().f1);
//            }
//        }).sum(0).print();
        Cores.map(new MapFunction<Vertex<Integer, Tuple3<Integer, Integer, Integer>>, Tuple4<Integer,Integer,Integer,Integer>>() {
            @Override
            public Tuple4<Integer, Integer, Integer, Integer> map(Vertex<Integer, Tuple3<Integer, Integer, Integer>> value) throws Exception {
                return new Tuple4<>(value.getId(), value.getValue().f0, value.getValue().f0, value.getValue().f0);
            }
        }).sortPartition(0,Order.ASCENDING).writeAsCsv(args[1], FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    public static class readGraph implements FlatMapFunction<String, Tuple3<Integer, Integer, NullValue>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, NullValue>> out) {
            if(value.equals("")) return;
            if(value.equals("\n")) return;
            if(value.charAt(0) == '#') return;
            String[] split = value.split("\\s+");
            out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), new NullValue()));
        }
    }
}
