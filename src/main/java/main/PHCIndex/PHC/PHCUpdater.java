package main.PHCIndex.PHC;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;

public class PHCUpdater<K> extends GatherFunction<K, PHCValue<K>, PHCMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, PHCValue<K>> vertex, MessageIterator<PHCMessage<K>> inMessages) {
        PHCValue<K> phcValue = new PHCValue<>(vertex.getValue());
        for (PHCMessage<K> message : inMessages) {
            phcValue.addNeighbor(message.getVertexId(), message.getCoreTime());
        }
        boolean updated = false;
        for (int time = 1; time < phcValue.getMaxTime(); time++) {
            for (int core = 0; core < phcValue.getCore(); core++) {
                if (phcValue.getCoreTime().get(time, core) == Integer.MAX_VALUE) {
                    break;
                }
                ArrayList<Integer> T = new ArrayList<>();
                for (PHCNeighborValue<K> neighborValue : phcValue.getNeighbors()) {
                    int t = neighborValue.getMaxEdgeTime();
                    if (t < time) continue;
                    T.add(Math.max(t, neighborValue.getCoreTimes().get(time, core)));
                }
                T.sort(Comparator.naturalOrder());
                int old = phcValue.getCoreTime().get(time, core);
                if (T.size() <= core) {
                    phcValue.getCoreTime().set(time, core, Integer.MAX_VALUE);
                } else {
                    phcValue.getCoreTime().set(time, core, T.get(core));
                }
                updated = updated || old != phcValue.getCoreTime().get(time, core);
            }
        }
        if (updated) {
            setNewVertexValue(phcValue);
        }
    }
}
