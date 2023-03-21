package USP;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSASingleSourceShortestPaths;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GraphLB {
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		List<Tuple2<String, String>> input = env.readTextFile("/Users/zhangwenqian/UNSW/frauddetection/input2.txt").flatMap(new WordConnect()).distinct().collect();
		DataSet<Tuple2<String, String>> edges = env.fromCollection(input);
		Graph<String, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env).getUndirected();
		System.out.println("total edges: " + graph.getEdges().count());
		DataSet<Vertex<String, ArrayList<String>>> result = new USP<>("abas", 10000).run(graph);
		result.flatMap(new FlatMapFunction<Vertex<String, ArrayList<String>>, Tuple2<Integer,String>>() {
			@Override
			public void flatMap(Vertex<String, ArrayList<String>> value, Collector<Tuple2<Integer, String>> collector) throws Exception {
				if(value.getId().contains("*")) {
					return;
				}
				if(value.getValue() == null) {
					collector.collect(new Tuple2<>(0, "No path to target " + value.getId()));
					return;
				}
				ArrayList<String> removedStar = new ArrayList<>();
				for(String s : value.getValue()) {
					if (!s.contains("*")) {
						removedStar.add(s);
					}
				}
				removedStar.add(value.getId());
				collector.collect(new Tuple2<>(removedStar.size(), removedStar.toString()));
			}
		}).sortPartition(0, Order.ASCENDING).print();
	}
	public static class WordConnect implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
			for(int i = 0; i < value.length(); i++) {
				out.collect(new Tuple2<>(value, value.substring(0, i) + "*" + value.substring(i + 1)));
			}
		}
	}
}
