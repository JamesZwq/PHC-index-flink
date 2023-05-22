package main.PHCIndex.CoreDecomposition;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

public class CDMessager<K extends Comparable<K>, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) {
        CDVertexValue<K> v = vertex.getValue();
        if (v.getChanged()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (v.getNeighbors().get(u) > v.getCore()) {
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore()));
                }
            }
        }
    }
}
