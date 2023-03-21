package main;

import USP.USP;
import main.PHCIndex.CoreDecomposition.CoreDecomposition;
import main.PHCIndex.PHCIndex;
import main.PHCIndex.PHCVertex.VertexValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        // set up the execution environment

//		Too few memory segments provided. Hash Table needs at least 33 memory segments.
		Configuration conf = new Configuration();
		conf.setInteger("taskmanager.memory.segment-size", 8192);
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = "/Users/zhangwenqian/UNSW/3901/PHC-index-flink/graphT.txt";
//        String path = "/Users/zhangwenqian/UNSW/3901/PHC-index-flink/graphTS.txt";
//        String path = "/Users/zhangwenqian/UNSW/3901/PHC-index-flink/iographT.txt";
//        String path = "/Users/zhangwenqian/Downloads/facebook/0_new.edges";
		DataSet<Tuple3<Integer, Integer, Integer>> edges = env.readTextFile(path).flatMap(new readGraph()).distinct();
		Graph<Integer, NullValue, Integer> graph = Graph.fromTupleDataSet(edges, env).getUndirected();

        new PHCIndex<Integer>(100).run(graph).sortPartition(0, Order.ASCENDING).setParallelism(1).print();

//        new SingleSourceShortestPaths<Integer, NullValue>(236, 1000).
//				run(Graph.fromTupleDataSet(env.
//						readTextFile("/Users/zhangwenqian/Downloads/facebook/0_new.edges").
//                flatMap(new readGraphTmp()).distinct(), env).getUndirected()).print();

//		new USP<Integer>(236,100).run(Graph.fromTupleDataSet(env
//				.readTextFile("/Users/zhangwenqian/Downloads/facebook/0_new.edges")
//				.flatMap(new readGraphUSP()).distinct(), env).getUndirected()).print();
    }

    public static class readGraph implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Integer>> out) {
            String[] split = value.split(" ");
            out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2])));
        }
    }

    public static class readGraphTmp implements FlatMapFunction<String, Tuple3<Integer, Integer, Double>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {
            String[] split = value.split(" ");
            out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Double.parseDouble(split[2])));
        }
    }


	public static class readGraphUSP implements FlatMapFunction<String, Tuple3<Integer, Integer, NullValue>> {
		@Override
		public void flatMap(String value, Collector<Tuple3<Integer, Integer, NullValue>> out) throws Exception {
			String[] split = value.split(" ");
			out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), NullValue.getInstance()));
		}
	}
}
