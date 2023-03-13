package main.PHCIndex.CoreDecomposition;

import main.PHCIndex.InitCT.InitCTMessage;
import main.PHCIndex.PHCVertex.VertexValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.List;

public class CDUpdater<K> extends GatherFunction<K, CDVertexValue<K>, CDMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CDVertexValue<K>> vertex, MessageIterator<CDMessage<K>> inMessages) throws Exception {
        CDVertexValue<K> v = new CDVertexValue<>(vertex.getValue());
        for(CDMessage<K> msg : inMessages) {
            v.setNeighbor(msg.getSource(), msg.getCore(), msg.getCnt());
        }
        v.setOldCore(v.getCore());
//        计算local core
        List<Integer> num = new ArrayList<>();
        for(int i = 0; i <= v.getCore(); i++){
            num.add(0);
        }

        for(K nei : v.getNeighbors().keySet()){
            int core = v.getNeighbors().get(nei).f0;
            int cnt = v.getNeighbors().get(nei).f1;
            if(core > v.getCore()){
                num.set(v.getCore(), num.get(v.getCore()) + 1);
            } else {
                num.set(core, num.get(core) + 1);
            }
        }

        int s = 0;
        for(int i = v.getOldCore(); i >= 0; i--){
            s += num.get(i);
            if(s >= i){
                v.setCore(i);
                break;
            }
        }

//        compute cnt
        s = 0;
        for (K nei : v.getNeighbors().keySet()) {
            int core = v.getNeighbors().get(nei).f0;
            if (core >= v.getCore()) {
                s++;
            }
        }
        v.setCnt(s);

//        update neighbors cnt

        for(K nei : v.getNeighbors().keySet()){
            Tuple2<Integer, Integer> u = v.getNeighbors().get(nei);
            if(u.f0 > v.getCore() && u.f1 <= v.getOldCore()){
                v.setNeighbor(nei, u.f0, u.f1-1);
            }
        }

        if(!v.equals(vertex.getValue()))
            setNewVertexValue(v);
    }
}
