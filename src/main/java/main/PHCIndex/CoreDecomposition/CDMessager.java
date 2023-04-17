package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;

public class CDMessager<K, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) {
//        System.out.println(vertex + "superstep: " + getSuperstepNumber());
        CDVertexValue<K> v = vertex.getValue();
        if (v.getCore() < v.getOldCore()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (v.getNeighbors().get(u) > v.getCore()) {
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore(), v.getOldCore(), false));
                }
            }
        }
//        System.out.println(toSend);
    }
}
