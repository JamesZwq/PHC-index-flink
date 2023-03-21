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
        if (getSuperstepNumber() == 1) {
            HashSet<K> set = new HashSet<>();
            for (Edge<K, EV> e : getEdges()) {
                if (set.contains(e.getTarget())) continue;
                set.add(e.getTarget());
                sendMessageTo(e.getTarget(), new CDMessage<>(vertex.getId(), v.getCore(), v.getCnt(), false));
            }
//            System.out.println("\033[33m" + "send message to " + set + " from " + vertex.getId() + "\033[0m");
        } else {
            boolean flag = false;
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (v.getNeighbors().get(u).f1 < v.getCore()) {
                    flag = true;
                    break;
                }
            }
            if (!flag) return;
            HashSet<K> set = new HashSet<>();
            for (K u : vertex.getValue().getNeighbors().keySet()) {
                if (set.contains(u)) continue;
                set.add(u);
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
//            System.out.println("\033[33m" + "send message to " + set + " from " + vertex.getId() + "\033[0m");
        }
    }
}
