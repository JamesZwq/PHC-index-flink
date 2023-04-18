package main.PHCIndex.CoreTime;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.HashSet;

public class CTMessager<K> extends ScatterFunction<K, CTValue<K>, CTMessage<K>, Integer> {
    @Override
    public void sendMessages(Vertex<K, CTValue<K>> vertex) {
        ArrayList<Boolean> updateAtOrigin = vertex.getValue().diffCoreTime();
        if(updateAtOrigin.contains(true)){
            ArrayList<NeighborValue<K>> nebrTimeMap = vertex.getValue().getNebrTimeMap();
            for(NeighborValue<K> neighborValue : nebrTimeMap){
                ArrayList<Boolean> updateAt = new ArrayList<>(updateAtOrigin);
                for(int i = 0; i < neighborValue.getCore() && i < vertex.getValue().getCore(); i++){
                    if(vertex.getValue().getCoreTime(i) <= neighborValue.getTime()) updateAt.set(i, false);
                }
                if (updateAt.contains(true))
                    sendMessageTo(neighborValue.getKey(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime(), updateAt));
                else {
                    break;
                }
            }
        }
    }
}
