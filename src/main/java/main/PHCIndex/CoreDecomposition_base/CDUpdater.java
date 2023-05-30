package main.PHCIndex.CoreDecomposition_base;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;

public class CDUpdater<K extends Comparable<K>> extends GatherFunction<K, CDVertexValue<K>, CDMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CDVertexValue<K>> vertex, MessageIterator<CDMessage<K>> inMessages) {
        CDVertexValue<K> v = vertex.getValue();
        v.setUnchanged();
        int oldCore = v.getCore();
        for (CDMessage<K> msg : inMessages) {
            v.setNeighbor(msg.getSource(), msg.getCore());
            v.numMsg++;

            ArrayList<Integer> cnts = new ArrayList<>();
            for (int i = 0; i <= v.getCore(); i++) {
                cnts.add(0);
            }

            for (K u : vertex.getValue().getNeighbors().keySet()) {
                int minCore = Math.min(v.getNeighbors().get(u), v.getCore());
                cnts.set(minCore, cnts.get(minCore) + 1);
            }

            int numCnt = 0;
            for (int i = v.getCore(); i >= 0; i--) {
                numCnt += cnts.get(i);
                if (numCnt >= i) {
                    v.setCore(i);
                    if (oldCore != i) {
                        v.setChanged();
                        v.completeAt = getSuperstepNumber();
                    }
                    break;
                }
            }
        }
        System.out.println("vertex " + vertex.getId() + " from " + oldCore + " to " + v.getCore() + " at superstep " + getSuperstepNumber());
        setNewVertexValue(v);
    }
}
