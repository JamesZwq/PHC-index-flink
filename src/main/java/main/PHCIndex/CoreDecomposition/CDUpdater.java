package main.PHCIndex.CoreDecomposition;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;

public class CDUpdater<K extends Comparable<K>> extends GatherFunction<K, CDVertexValue<K>, CDMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CDVertexValue<K>> vertex, MessageIterator<CDMessage<K>> inMessages) {
        CDVertexValue<K> v = vertex.getValue();
        v.setUnchanged();
        for (CDMessage<K> msg : inMessages) {
            if (msg.getCore() >= v.getCore()) {
                v.setCnt(v.getCnt() + 1);
            } else {
                v.setCnts(msg.getCore(), v.getCnts()[msg.getCore()] + 1);
            }

            int nbrK = v.getNeighbors().get(msg.getSource());
            if(nbrK >= v.getCore()){
                v.setCnt(v.getCnt() - 1);
            } else {
                v.setCnts(nbrK, v.getCnts()[nbrK] - 1);
            }

            v.setNeighbor(msg.getSource(), msg.getCore());
            v.numMsg++;
        }
        int numCnt = v.getCnt();

        if(numCnt >= v.getCore()) {
            setNewVertexValue(v);
            return;
        }
        v.setChanged();
        for (int i = v.getCore()-1; i >= 0; i--) {
            numCnt += v.getCnts()[i];
            if (numCnt >= i) {
                v.setCore(i);
                v.setCnt(numCnt);
                break;
            }
        }
        setNewVertexValue(v);
    }
}
