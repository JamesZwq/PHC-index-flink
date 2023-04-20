package main.PHCIndex.CoreDecomposition;

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
            int minCore = Math.min(msg.getCore(),v.getCnts().size()-1);
            v.setCnts(minCore, v.getCnts().get(minCore) + 1);

            if (minCore >= v.getOldCore()) {
                v.setCnt(v.getCnt() + 1);
            }

            int minOldCore = Math.min(v.getNeighbors().get(msg.getSource()),v.getCnts().size()-1);
            v.setCnts(minOldCore, v.getCnts().get(minOldCore) - 1);

            if(minOldCore >= v.getOldCore()){
                v.setCnt(v.getCnt() - 1);
            }

            v.setNeighbor(msg.getSource(), msg.getCore());
        }

        int numCnt = v.getCnt();
        for (int i = v.getCore(); i < v.getOldCore(); i++) {
            numCnt += v.getCnts().get(i);
        }
        v.setCnt(numCnt);

        v.setOldCore(v.getCore());
        if(numCnt >= v.getCore()) {
            setNewVertexValue(v);
            return;
        }
        for (int i = v.getCore() - 1; i >= 0; i--) {
            numCnt += v.getCnts().get(i);
            if (numCnt >= i) {
                v.setCore(i);
                break;
            }
        }
        setNewVertexValue(v);
    }
}
