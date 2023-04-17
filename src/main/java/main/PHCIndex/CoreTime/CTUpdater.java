package main.PHCIndex.CoreTime;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;

public class CTUpdater<K> extends GatherFunction<K, CTValue<K>, CTMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CTValue<K>> vertex, MessageIterator<CTMessage<K>> inMessages) {
        CTValue<K> value = new CTValue<>(vertex.getValue());
        ArrayList<Boolean> updateAt = vertex.getValue().diffCoreTime();
        for (CTMessage<K> message : inMessages) {
            value.addNebrTimeMap(message.getSource(), message.getCore(), message.getCoreTime());
            for (int i = 0; i < message.getCore() && i < value.getCore(); i++) {
                if (message.getUpdateAt().get(i)) updateAt.set(i, true);
            }
        }
        value.setOldCoreTime(value.getCoreTime());
        for (int k = value.getCore() - 1; k >= 0; k--) {
            if (!updateAt.get(k)) continue;
            ArrayList<Integer> times = new ArrayList<>();
            int cnt = 0;
            for (NeighborValue<K> kNeighborValue : value.getNebrTimeMap()) {
                if (kNeighborValue.getCore() <= k) continue;
                if (Math.max(kNeighborValue.getTime(), kNeighborValue.getCoreTime(k)) <= value.getCoreTime(k)) {
                    cnt++;
                    if (cnt > k) break;
                }
            }
            if (cnt > k) {
                continue;
            }
            for (int i = 0; i < value.getNebrTimeMap().size(); i++) {
                NeighborValue<K> kNeighborValue = value.getNebrTimeMap().get(i);
                if (kNeighborValue.getCore() <= k) continue;
//                compare the min time of the kth core of the neighbors
                times.add(Math.max(kNeighborValue.getTime(), kNeighborValue.getCoreTime(k)));
            }
            times.sort(Comparator.naturalOrder());
            value.setCoreTime(k, times.get(k));
        }
        setNewVertexValue(value);
    }
}
