package main.PHCIndex.CoreDecomposition_base;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;

public class CDMessager<K extends Comparable<K>, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) {
        CDVertexValue<K> v = vertex.getValue();
        ArrayList<K> sendList = new ArrayList<>();
        if (v.getChanged()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
//                if (v.getNeighbors().get(u) > v.getCore() || getSuperstepNumber() == 1) {
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore()));
                    sendList.add(u);
//                }
            }
            System.out.println("from " + vertex.getId() + " send " + sendList + " at superstep " + getSuperstepNumber());
        }
    }
}
