package main.PHCIndex.PHC_times.PHC;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;

import static main.PHCIndex.PHC_times.PHC.PHC_times.numUpdates;

public class PHCUpdater<K> extends GatherFunction<K, PHCValue<K>, PHCMessage<K>> {
    private final int time;

    public PHCUpdater(int time) {
        this.time = time;
    }

    @Override
    public void updateVertex(Vertex<K, PHCValue<K>> vertex, MessageIterator<PHCMessage<K>> inMessages) {
        PHCValue<K> phcValue = new PHCValue<>(vertex.getValue());
        phcValue.setUpdated(false);
        phcValue.setOldCTtoCT();
        phcValue.setCore(phcValue.getNewCore());
        ArrayList<Boolean> updatedAt = new ArrayList<>();
        for(int i = 0; i < phcValue.getCore(); i++){
            updatedAt.add(false);
        }
        for (PHCMessage<K> message : inMessages) {
            phcValue.addNeighbor(message.getVertexId(), message.getCoreTime(), message.getCore());
            for(int i = 0; i < Math.min(message.getUpdateAt().size(), phcValue.getCore()); i++){
                if(message.getUpdateAt().get(i)) {
                    updatedAt.set(i, true);
                }
            }
        }
        boolean updated = false;
        int numRedused = 0;
        for (int core = 1; core < phcValue.getCore(); core++) {
            if (!updatedAt.get(core)) continue;
            if (phcValue.getCoreTime().get(core) == Integer.MAX_VALUE) {
                break;
            }
            ArrayList<Integer> T = new ArrayList<>();
            for (PHCNeighborValue<K> neighborValue : phcValue.getNeighbors()) {
                int t = neighborValue.getMaxEdgeTime();
                if (t < time) continue;
                T.add(Math.max(neighborValue.getTimeAfter(time), neighborValue.getCoreTime().get(core)));
            }
            T.sort(Comparator.naturalOrder());
            if (T.size() <= core) {
                phcValue.getCoreTime().set(core, Integer.MAX_VALUE);
                numRedused++;
            } else {
                phcValue.getCoreTime().set(core, T.get(core));
            }
            if(!Objects.equals(phcValue.getOldCoreTimes().get(core), phcValue.getCoreTime().get(core))){
                updated = true;
                phcValue.setUpdated(true);
                phcValue.addPHC_Index(core, time, phcValue.getCoreTime().get(core));
                System.out.println("\033[0;32m" + "superstep " + getSuperstepNumber() +
                        " at core " + core +
                        " v: "+ vertex.getId() +
                        " value (" + time + ", " + phcValue.getCoreTime().get(core) + ")" + "\033[0m");
            } else {
                numUpdates++;
                System.out.println("\033[0;31m" + "superstep " + getSuperstepNumber() +
                        " at core " + core + " v: "+ vertex.getId() + " value (" + time + ", " + phcValue.getCoreTime().get(core) + ")" + "\033[0m");
                System.out.println("\033[0;31m" + "numUpdates = " + numUpdates + "\033[0m");
            }
        }
        for(int i = 0; i < numRedused; i++){
            phcValue.reduceCore();
            System.out.println("Reduced superstep " + getSuperstepNumber() + " v: "+ vertex.getId() + " numRedused = " + numRedused + " core = " + phcValue.getCore() + " newCore = " + phcValue.getNewCore());
        }
        setNewVertexValue(phcValue);
    }
}
