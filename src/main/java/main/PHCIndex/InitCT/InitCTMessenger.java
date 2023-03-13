package main.PHCIndex.InitCT;


import main.PHCIndex.PHCVertex.NeighborsValue;
import main.PHCIndex.PHCVertex.VertexValue;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.HashSet;
/**
 * @param <K> The type of the vertex key.
 */
public final class InitCTMessenger<K>
        extends ScatterFunction<K, VertexValue<K>, InitCTMessage<K>, Integer> {
    private final int timeEnd;

    public InitCTMessenger(int timeStart) {
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
                    sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
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
                    sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), true));
                    visited.add(e.getTarget());
                } else if (vertex.getValue().getCore() != vertex.getValue().getOldCore()) {
                    sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
                    visited.add(e.getTarget());
                }
            }
        }
    }
}