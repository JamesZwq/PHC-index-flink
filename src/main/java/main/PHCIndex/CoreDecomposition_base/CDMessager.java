package main.PHCIndex.CoreDecomposition_base;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

public class CDMessager<K extends Comparable<K>, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) {
        CDVertexValue<K> v = vertex.getValue();
        if (v.getCore() < v.getOldCore()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore()));
            }
        }
    }
}
