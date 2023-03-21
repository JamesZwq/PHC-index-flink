package main.PHCIndex.CoreTime;

import main.PHCIndex.CoreDecomposition.CDVertexValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public class CTUpdater<K> extends GatherFunction<K, CTvalue<K>, CTMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CTvalue<K>> vertex, MessageIterator<CTMessage<K>> inMessages) throws Exception {
//        System.out.println("superstep: " + getSuperstepNumber());
//        CTvalue<K> v = new CTvalue<>(vertex.getValue());
//        for (CTMessage<K> msg : inMessages) {
//            v.setNeighbor(msg.getSource(), msg.getCore(), msg.getCoreTime());
//        }
//        ArrayList<Integer> oldTime = v.getCoreTime();
//        v.setOldCoreTime(oldTime);
////        calculate local core time
//        int ub = 0;
//        for(int k = 0; k < v.getCore(); k++) {
//            HashSet<Integer> T = new HashSet<>();
//            HashSet<K> visited = new HashSet<>();
//            for (K u : v.getNeighbors().keySet()) {
//                if (visited.contains(u)) continue;
//                if (v.getNeighbors().get(u).f0 < v.getCore()) continue;
//                int t = v.getNeighbors().get(u).f1.get(k);
//                if (T.size() >= v.getCore()) break;
//                visited.add(u);
//                int ct;
//                ct = Math.max(t, v.getNeighbors().get(u).f1.get(k));
//                T.add(ct);
//                if (T.size() <= v.getCore()) ub = Math.max(ub, ct);
//            }
//            v.setCoreTime(k, T.stream().limit(v.getCore()).min(Comparator.naturalOrder()).get());
//        }
////        update global core time
//        setNewVertexValue(v);
    }
}
