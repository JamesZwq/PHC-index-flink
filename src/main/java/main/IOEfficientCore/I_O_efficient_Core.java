/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main.IOEfficientCore;

import main.IOEfficientCore.MyMessage;
import main.IOEfficientCore.MyVertex;
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
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static java.lang.Math.min;

/**
 * This is an implementation of the Single-Source-Shortest Paths algorithm, using a scatter-gather
 * iteration.
 */
@SuppressWarnings("serial")
public class I_O_efficient_Core<K, EV>
		implements GraphAlgorithm<K, NullValue, EV, DataSet<Vertex<K, MyVertex<K>>>> {
	private final Integer maxIterations;
	private final HashMap<K, Integer> tmp_core;

	private final HashMap<K, HashSet<K>> neighbors;

	/**
	 * Creates an instance of the SingleSourceShortestPaths algorithm.
	 *
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public I_O_efficient_Core(Integer maxIterations) {
		this.maxIterations = maxIterations;
		this.tmp_core = new HashMap<>();
		this.neighbors = new HashMap<>();
	}

	static private int ComputeCnt(ArrayList<Integer> nbrCore, int oldCore) {
		return nbrCore.stream().filter(x -> x >= oldCore).mapToInt(x -> 1).sum();
	}

	static private int LocalCore(int currCode, ArrayList<Integer> nbrCore) {
		ArrayList<Integer> num = new ArrayList<>();
		for (int i = 0; i < currCode; i++) {
			num.add(0);
		}
		for (int i : nbrCore) {
			num.set(min(i, currCode) - 1, num.get(min(i, currCode)  - 1) + 1);
		}
		int sum = 0;
		int i = currCode - 1;
		for (; i >= 0; i--) {
			sum += num.get(i);
			if (sum >= i+1) break;
		}
		return i + 1;
	}

	@Override
	public DataSet<Vertex<K, MyVertex<K>>> run(Graph<K, NullValue, EV> input) throws Exception {
		for (Edge<K, EV> edge : input.getEdges().collect()) {

			if(neighbors.containsKey(edge.getSource())) {
				neighbors.get(edge.getSource()).add(edge.getTarget());
			} else {
				HashSet<K> tmp = new HashSet<>();
				tmp.add(edge.getTarget());
				neighbors.put(edge.getSource(), tmp);
			}

			if (tmp_core.containsKey(edge.getSource())) {
				tmp_core.put(edge.getSource(), tmp_core.get(edge.getSource()) + 1);
			} else {
				tmp_core.put(edge.getSource(), 1);
			}

		}

		return input.mapVertices(new InitVerticesMapper<>(tmp_core, neighbors))
				.runScatterGatherIteration(
						new MinDistanceMessenger<>(), new VertexDistanceUpdater<>(), maxIterations)
				.getVertices();
	}

	private static final class InitVerticesMapper<K, VV>
			implements MapFunction<Vertex<K, VV>, MyVertex<K>> {

		private final HashMap<K, Integer> tmp_core;
		private final HashMap<K, HashSet<K>> neighbors;

		public InitVerticesMapper(HashMap<K, Integer> tmp_core, HashMap<K, HashSet<K>> neighbors) {
			this.tmp_core = tmp_core;
			this.neighbors = neighbors;
		}

		public MyVertex<K> map(Vertex<K, VV> value) {

			HashMap<K, Tuple2<Integer,Integer>> curr_bre = new HashMap<>();

			for (K neighbor : neighbors.get(value.getId())) {
				curr_bre.put(neighbor, new Tuple2<>(tmp_core.get(neighbor),0));
			}

			return new MyVertex<K>(value.getId(), tmp_core.get(value.getId()), 0, curr_bre);
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all the target vertices
	 * summed up with the edge's value.
	 *
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K,EV>
			extends ScatterFunction<K, MyVertex<K>, MyMessage<K>, EV> {
		@Override
		public void sendMessages(Vertex<K, MyVertex<K>> vertex) {
			if (vertex.getValue() != null) {
				for (Edge<K, EV> edge : getEdges()) {
					MyMessage<K> myMessage = new MyMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCut());
					sendMessageTo(edge.getTarget(), myMessage);
				}
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the shortest Array from all incoming
	 * messages.
	 *
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K> extends GatherFunction<K, MyVertex<K>, MyMessage<K>> {

		private HashMap<K, Integer> UpdateNbrCnt(HashMap<K, Tuple2<Integer,Integer>> neighbors, int oldCore, int newCore) {
			HashMap<K, Integer> nbrCnt = new HashMap<>();
			for (K neighbor : neighbors.keySet()) {
				if (neighbors.get(neighbor).f0 > newCore && neighbors.get(neighbor).f0 <= oldCore) {
					nbrCnt.put(neighbor, neighbors.get(neighbor).f1 - 1);
				}
			}
			return nbrCnt;
		}
		@Override
		public void updateVertex(Vertex<K, MyVertex<K>> vertex, MessageIterator<MyMessage<K>> inMessages) {
			MyVertex<K> newVertexValue = new MyVertex<>(vertex.getValue());
			for (MyMessage<K> msg : inMessages) {
				if (msg != null) {
					newVertexValue.getNeighbors().put(msg.getVertex(), new Tuple2<>(msg.getCore(), msg.getCnt()));
				}
			}
			if (vertex.getValue().getCut() >= vertex.getValue().getCore()) {
				if (!newVertexValue.equals(vertex.getValue())) {
					setNewVertexValue(newVertexValue);
					return;
				}
			}

			ArrayList<Integer> nbrCore = new ArrayList<>();
			for (Tuple2<Integer, Integer> nbr : newVertexValue.getNeighbors().values()) {
				nbrCore.add(nbr.f0);
			}

			int oldCore = newVertexValue.getCore();
			int newCore = LocalCore(oldCore, nbrCore);

			newVertexValue.setCore(newCore);

			newVertexValue.setCut(ComputeCnt(nbrCore, newCore));

			HashMap<K, Integer> nbrCnt = UpdateNbrCnt(newVertexValue.getNeighbors(), oldCore, newCore);

			for (K nbr : nbrCnt.keySet()) {
				newVertexValue.getNeighbors().put(nbr, new Tuple2<>(newVertexValue.getNeighbors().get(nbr).f0, nbrCnt.get(nbr)));
			}
			if (!newVertexValue.equals(vertex.getValue())) {
				setNewVertexValue(newVertexValue);
			}
		}
	}
}
