package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

public class CDMessager<K, EV> extends ScatterFunction<K, CDVertexValue<K>, CDMessage<K>, EV> {
    @Override
    public void sendMessages(Vertex<K, CDVertexValue<K>> vertex) throws Exception {
        CDVertexValue<K> v = vertex.getValue();
        if (getSuperstepNumber() == 1) {
            for (Edge<K, EV> e : getEdges()) {
                sendMessageTo(e.getTarget(), new CDMessage<>(vertex.getId(), v.getCore(), v.getCnt(), false));
            }
        } else {
            boolean flag = false;
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (v.getNeighbors().get(u).f1 < v.getCore()) {
                    flag = true;
                    break;
                }
            }
            if (!flag) return;
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                Tuple2<Integer, Integer> currNei = v.getNeighbors().get(u);
                int core = currNei.f0;
                if (core > v.getCore() && core <= v.getOldCore()) {
                    sendMessageTo(u, new CDMessage<>(vertex.getId(), v.getCore(), v.getCnt(), false));
                }
            }
        }
    }
}
