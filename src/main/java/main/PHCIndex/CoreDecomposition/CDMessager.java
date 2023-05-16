package main.PHCIndex.CoreDecomposition;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.Map;

public class CDMessager<K extends Comparable<K>, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) {
        CDVertexValue<K> v = vertex.getValue();
        if (v.getCore() < v.getOldCore()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (v.getNeighbors().get(u) > v.getCore()) {
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore()));
                }
            }
//            for (Map.Entry<K, Integer> u : vertex.getValue().getNeighbors().entrySet()) {
//                if (u.getValue() > v.getCore()) {
//                    sendMessageTo(u.getKey(), new CDMessage<>(vertex.getId(), v.getCore()));
//                }
//            }
        }
    }
}
