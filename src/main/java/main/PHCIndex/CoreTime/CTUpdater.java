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
        CTvalue<K> value = new CTvalue<>(vertex.getValue());
        for (CTMessage<K> message : inMessages) {
            value.addNebrTimeMap(message.getSource(), message.getCore(), message.getCoreTime());
        }
        for(int k = value.getCore()-1; k >= 0; k--){
            ArrayList<Integer> times = new ArrayList<>();
            for(int i = 0; i < value.getNebrTimeMap().size(); i++){
                NeighborValue<K> kNeighborValue = value.getNebrTimeMap().get(i);
                if (kNeighborValue.getCore() <= k) continue;
                times.add(Math.max(kNeighborValue.getTime(), kNeighborValue.getCoreTime(k)));
            }
            times.sort(Comparator.naturalOrder());
            value.setCoreTime(k, times.get(k));
        }

        if (!value.equals(vertex.getValue())) {
            setNewVertexValue(value);
        }
    }
}
