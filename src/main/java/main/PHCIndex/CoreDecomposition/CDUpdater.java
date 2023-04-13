package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.List;

public class CDUpdater<K> extends GatherFunction<K, CDVertexValue<K>, CDMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CDVertexValue<K>> vertex, MessageIterator<CDMessage<K>> inMessages) {
        CDVertexValue<K> v = new CDVertexValue<>(vertex.getValue());
//        boolean decreased = false;
        for (CDMessage<K> msg : inMessages) {
            v.setNeighbor(msg.getSource(), msg.getCore(), msg.getCnt());
        }

        int t = 0;
        for (K nei : v.getNeighbors().keySet()) {
            int core = v.getNeighbors().get(nei).f0;
            if (core >= v.getCore()) {
                t++;
            }
        }

        v.setCnt(t);

        if(v.getCnt() >= v.getCore()) {
            setNewVertexValue(v);
            return;
        }

        v.setOldCore(v.getCore());
//        计算local core
        List<Integer> num = new ArrayList<>();
        for (int i = 0; i <= v.getCore(); i++) {
            num.add(0);
        }

        for (K nei : v.getNeighbors().keySet()) {
            int core = v.getNeighbors().get(nei).f0;
            int min = Math.min(core, v.getCore());
            num.set(min, num.get(min) + 1);
        }

        int s = 0;
        for (int i = v.getOldCore(); i >= 0; i--) {
            s += num.get(i);
            if (s >= i) {
                v.setCore(i);
                break;
            }
        }

        if(v.getCore() != v.getOldCore()){
            System.out.println("core: " + v.getCore() + " oldCore: " + v.getOldCore());
        } else {
            System.out.println("core: " + v.getCore() + " oldCore: " + v.getOldCore() + " no change");
        }
        if (!v.equals(vertex.getValue())) {
            setNewVertexValue(v);
        }
    }
}
