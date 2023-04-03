package main;

import main.PHCIndex.PHCIndex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        Configuration conf = new Configuration();
        conf.setInteger("taskmanager.memory.segment-size", 4096);
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = "/Users/zhangwenqian/UNSW/3901/PHC-index-flink/sortedGraphT.txt";
//        String path = "/Users/zhangwenqian/Downloads/facebook/0_new_sort.edges";
        DataSet<Tuple3<Integer, Integer, Integer>> edges = env.readTextFile(path).flatMap(new readGraph()).distinct();
        Graph<Integer, NullValue, Integer> graph = Graph.fromTupleDataSet(edges, env).getUndirected();
//        new PHCIndex<Integer>(10).run(graph).sortPartition(0, Order.ASCENDING).print();
        new PHCIndex<Integer>(100).run(graph);
    }

    public static class readGraph implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Integer>> out) {
            String[] split = value.split(" ");
//            int time = Integer.parseInt(split[2]);
//            int start = 2;
//            int end = 8;
//            if(time >= start && time <= end){
            out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2])));
//            }
        }
    }
}
