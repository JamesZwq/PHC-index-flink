package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.HashSet;

public class CDMessager<K, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) throws Exception {
        CDVertexValue<K> v = vertex.getValue();
        if(v.getCore() < v.getOldCore()) {
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                Tuple2<Integer, Integer> currNei = v.getNeighbors().get(u);
                int core = currNei.f0;
                if (core > v.getCore() && core <= v.getOldCore()) {
//                    如果需要减少cut(u)的值，那么就发送true
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore(), v.getCnt(), true));
                } else {
//                    如果不需要减少cut(u)的值，那么就发送false
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore(), v.getCnt(), false));
                }
            }
        }
    }
}
