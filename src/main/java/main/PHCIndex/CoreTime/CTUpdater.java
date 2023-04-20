package main.PHCIndex.CoreTime;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;

public class CTUpdater<K extends Comparable<K>> extends GatherFunction<K, CTValue<K>, CTMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CTValue<K>> vertex, MessageIterator<CTMessage<K>> inMessages) {
        CTValue<K> value = new CTValue<>(vertex.getValue());
        ArrayList<K> resFrom = new ArrayList<>();
        for (CTMessage<K> message : inMessages) {
            resFrom.add(message.getSource());
            value.addNebrTimeMap(message.getSource(), message.getCore(), message.getCoreTime());
        }
        resFrom.sort(Comparator.naturalOrder());
        System.out.println("\u001B[32m" + vertex.getId()  + " resFrom: \n" + resFrom + "\u001B[0m");
        value.setOldCoreTime(value.getCoreTime());
        for (int k = 0; k < value.getCore(); k++) {
            ArrayList<Integer> times = new ArrayList<>();
            for (int i = 0; i < value.getNebrTimeMap().size(); i++) {
                NeighborValue<K> kNeighborValue = value.getNebrTimeMap().get(i);
                if (kNeighborValue.getCore() > k){
                    times.add(Math.max(kNeighborValue.getTime(), kNeighborValue.getCoreTime(k)));
                }
            }
            times.sort(Comparator.naturalOrder());
            value.setCoreTime(k, times.get(k));
        }
        System.out.println("id " + vertex.getId() + " coreTime \n" + value.getCoreTime() + "\n oldCoreTime \n" + value.getOldCoreTime());
        System.out.println("updated: " + !value.getCoreTime().equals(value.getOldCoreTime()));
        setNewVertexValue(value);
    }
}
