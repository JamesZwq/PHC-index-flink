package main.PHCIndex.PHC.InitCT;


import main.PHCIndex.NeighborsValue;
import main.PHCIndex.VertexValue;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public final class PHCMessenger<K>
        extends ScatterFunction<K, VertexValue<K>, PHCMessage<K>, Integer> {
    private final int timeEnd;

    public PHCMessenger(int timeStart) {
        this.timeEnd = timeStart;
    }

    @Override
    public void preSuperstep() throws Exception {
        super.preSuperstep();
    }

    @Override
    public void sendMessages(Vertex<K, VertexValue<K>> vertex) throws Exception {
        HashSet<K> visited = new HashSet<>();
        if(!vertex.getValue().isCalculated()) {
            for (Edge<K, Integer> e : getEdges()) {
                if (e.getValue() > timeEnd) continue;
//                    if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                if (visited.contains(e.getTarget())) continue;
                NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                if(!neighborsValue.decreaseCTN()){
                    sendMessageTo(e.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
                    visited.add(e.getTarget());
                }
            }
        } else {
            for (Edge<K, Integer> e : getEdges()) {
                if (e.getValue() > timeEnd) continue;
//                    if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                if (visited.contains(e.getTarget())) continue;
                NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                if (vertex.getValue().getCore() < neighborsValue.getCore() && neighborsValue.getCore() <= vertex.getValue().getOldCore()) {
                    sendMessageTo(e.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCore(), true));
                    visited.add(e.getTarget());
                } else if (vertex.getValue().getCore() != vertex.getValue().getOldCore()) {
                    sendMessageTo(e.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
                    visited.add(e.getTarget());
                }
            }
        }
    }
}