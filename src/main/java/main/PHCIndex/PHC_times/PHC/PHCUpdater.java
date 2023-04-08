package main.PHCIndex.PHC_times.PHC;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;

public class PHCUpdater<K> extends GatherFunction<K, PHCValue<K>, PHCMessage<K>> {
    private int time;

    public PHCUpdater(int time) {
        this.time = time;
    }

    @Override
    public void updateVertex(Vertex<K, PHCValue<K>> vertex, MessageIterator<PHCMessage<K>> inMessages) {
        PHCValue<K> phcValue = new PHCValue<>(vertex.getValue());
        phcValue.setOldCTtoCT();
        for (PHCMessage<K> message : inMessages) {
            phcValue.addNeighbor(message.getVertexId(), message.getCoreTime());
        }
        boolean updated = false;
        for (int core = 1; core < phcValue.getCore(); core++) {
            if (phcValue.getCoreTime().get(core) == Integer.MAX_VALUE) {
                break;
            }
            ArrayList<Integer> T = new ArrayList<>();
            for (PHCNeighborValue<K> neighborValue : phcValue.getNeighbors()) {
                int t = neighborValue.getMaxEdgeTime();
                if (t < time) continue;
                T.add(Math.max(neighborValue.getTimeAfter(time), neighborValue.getCoreTimes().get(core)));
            }
            T.sort(Comparator.naturalOrder());
            if (T.size() <= core) {
                phcValue.getCoreTime().set(core, Integer.MAX_VALUE);
            } else {
                phcValue.getCoreTime().set(core, T.get(core));
            }
            if(!Objects.equals(phcValue.getOldCoreTimes().get(core), phcValue.getCoreTime().get(core))){
                updated = true;
                phcValue.addPHC_Index(core, time, phcValue.getCoreTime().get(core));
            } else {
                System.out.println("Not updated");
            }
        }
        if (updated) {
            setNewVertexValue(phcValue);
        }
    }
}
