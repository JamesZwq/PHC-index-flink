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
        ArrayList<K> toSend = new ArrayList<>();
        for (CDMessage<K> msg : inMessages) {
            toSend.add(msg.getSource());
            v.setNeighbor(msg.getSource(), msg.getCore());

//            int minCore = Math.min(msg.getCore(), v.getCnts().size()-1);
//            v.setCnts(minCore, v.getCnts().get(minCore) + 1);
            if(msg.getCore() <= v.getCnts().size()-1){
                v.setCnts(msg.getCore(), v.getCnts().get(msg.getCore()) + 1);
            }

            if(msg.getOldCore() >= v.getOldCore()){
                v.setCnt(v.getCnt() - 1);
            }

//            int minOldCore = Math.min(msg.getOldCore(), v.getCnts().size()-1);
//            v.setCnts(minOldCore, v.getCnts().get(minOldCore) - 1);
            if (msg.getOldCore() <= v.getCnts().size()-1) {
                v.setCnts(msg.getOldCore(), v.getCnts().get(msg.getOldCore()) - 1);
            }

            if(msg.getCore() >= v.getOldCore()){
                v.setCnt(v.getCnt() + 1);
            }
        }
//        System.out.println(vertex.getId() + " " + toSend);

        int numCnt = v.getCnt();
        for (int i = v.getCore(); i < v.getOldCore(); i++) {
            numCnt += v.getCnts().get(i);
        }
        v.setCnt(numCnt);

        if(numCnt >= v.getCore()) {
            v.setOldCore(v.getCore());
            setNewVertexValue(v);
            return;
        }
        v.setOldCore(v.getCore());
//        计算local core
        List<Integer> num = new ArrayList<>();
        for (int i = 0; i <= v.getCore(); i++) {
            num.add(0);
        }

        for (K nei : v.getNeighbors().keySet()) {
            int core = v.getNeighbors().get(nei);
            int min = Math.min(core, v.getCore());
            num.set(min, num.get(min) + 1);
        }

        int s = 0;
        for (int i = v.getOldCore(); i >= 0; i--) {
            s += num.get(i);
            if (s >= i) {
                v.setCore(i);
                break;
            }
        }
//        System.out.println(vertex.getId() + " " + v);
        if (!v.equals(vertex.getValue())) {
            setNewVertexValue(v);
        }
    }
}
