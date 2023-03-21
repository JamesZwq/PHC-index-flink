package main.PHCIndex.CoreTime;

import main.PHCIndex.CoreDecomposition.CDVertexValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.HashSet;

public class CTMessager<K> extends ScatterFunction<K, CTvalue<K>, CTMessage<K>, Integer> {
    @Override
    public void sendMessages(Vertex<K, CTvalue<K>> vertex) throws Exception {
//        if(getSuperstepNumber() == 1){
//            HashSet<K> set = new HashSet<>();
//            for(Edge<K, Integer> e : getEdges()){
//                if(set.contains(e.getTarget())) continue;
//                set.add(e.getTarget());
//                sendMessageTo(e.getTarget(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
//            }
//        } else {
//            if(!ListCompare(vertex.getValue().getCoreTime(), vertex.getValue().getOldCoreTime())){
//                HashSet<K> set = new HashSet<>();
//                for(K u : vertex.getValue().getNeighbors().keySet()){
//                    if(set.contains(u)) continue;
//                    set.add(u);
//                    sendMessageTo(u, new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
//                }
//            };
//        }
    }

    static boolean ListCompare(ArrayList<? extends Comparable> a, ArrayList<? extends Comparable> b){
        if(a.size() != b.size()) return false;
        for(int i = 0; i < a.size(); i++){
            if(a.get(i) != b.get(i)) return false;
        }
        return true;
    }
}
