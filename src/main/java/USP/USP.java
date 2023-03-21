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

package USP;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;

/**
 * This is an implementation of the Single-Source-Shortest Paths algorithm, using a scatter-gather
 * iteration.
 */
@SuppressWarnings("serial")
public class USP<K>
		implements GraphAlgorithm<K, NullValue, NullValue, DataSet<Vertex<K, ArrayList<K>>>> {

	private final K srcVertexId;
	private final Integer maxIterations;

	/**
	 * Creates an instance of the SingleSourceShortestPaths algorithm.
	 *
	 * @param srcVertexId The ID of the source vertex.
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public USP(K srcVertexId, Integer maxIterations) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, ArrayList<K>>> run(Graph<K, NullValue, NullValue> input) {

		return input.mapVertices(new InitVerticesMapper<>(srcVertexId))
				.runScatterGatherIteration(
						new MinDistanceMessenger<>(), new VertexDistanceUpdater<>(), maxIterations)
				.getVertices();
	}

	private static final class InitVerticesMapper<K, VV>
			implements MapFunction<Vertex<K, VV>, ArrayList<K>> {

		private final K srcVertexId;

		public InitVerticesMapper(K srcId) {
			this.srcVertexId = srcId;
		}

		public ArrayList<K> map(Vertex<K, VV> value) {
			if (value.f0.equals(srcVertexId)) {
				return new ArrayList<>();
			} else {
				return null;
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all the target vertices
	 * summed up with the edge's value.
	 *
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K>
			extends ScatterFunction<K, ArrayList<K>, ArrayList<K>, NullValue> {

		@Override
		public void sendMessages(Vertex<K, ArrayList<K>> vertex) {
//            send the distance to all the neighbors
			if (vertex.getValue() != null) {
				for (Edge<K, NullValue> edge : getEdges()) {
					ArrayList<K> path = new ArrayList<>(vertex.getValue());
					path.add(edge.getSource());
					sendMessageTo(edge.getTarget(), path);
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
	public static final class VertexDistanceUpdater<K> extends GatherFunction<K, ArrayList<K>, ArrayList<K>> {

		@Override
		public void updateVertex(Vertex<K, ArrayList<K>> vertex, MessageIterator<ArrayList<K>> inMessages) {
//          find the shortest path in the messages
			ArrayList<K> minDistance = null;
			int size = 0;
			for (ArrayList<K> msg : inMessages) {
				if (msg != null && (minDistance == null || msg.size() < minDistance.size())) {
					minDistance = new ArrayList<>(msg);
				}
				size++;
			}
			System.out.println("Vertex: " + vertex.getId() + " Size: " + size);
//          update the vertex value if the received distance is smaller
			if (vertex.getValue() == null || (minDistance != null && vertex.getValue().size() > minDistance.size())) {
				setNewVertexValue(minDistance);
			}
		}
	}
}
