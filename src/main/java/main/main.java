package main;

import main.IOEfficientCore.I_O_efficient_Core;
import main.PHCIndex.PHCIndex;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.library.GSASingleSourceShortestPaths;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class main {
	public static void main(String[] args) throws Exception {
		// set up the execution environment

//		Too few memory segments provided. Hash Table needs at least 33 memory segments.
		Configuration conf = new Configuration();
		conf.setInteger("taskmanager.memory.segment-size", 4096);

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
		DataSet<Tuple3<Integer, Integer, Integer>> edges = env.readTextFile("graphT.txt").flatMap(new readGraph()).distinct();
		Graph<Integer, NullValue, Integer> graph = Graph.fromTupleDataSet(edges, env).getUndirected();
		new PHCIndex<Integer>(100).run(graph).sortPartition(0, Order.ASCENDING).setParallelism(1).print();
//		new I_O_efficient_Core<Integer,Integer>(100).run(graph).sortPartition(0, Order.ASCENDING).setParallelism(1).print();
	}
	public static class readGraph implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple3<Integer, Integer, Integer>> out) {
			String[] split = value.split(" ");
			out.collect(new Tuple3<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2])));
		}
	}
}
