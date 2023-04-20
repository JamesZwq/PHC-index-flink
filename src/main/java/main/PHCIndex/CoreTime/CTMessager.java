package main.PHCIndex.CoreTime;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

public class CTMessager<K extends Comparable<K>> extends ScatterFunction<K, CTValue<K>, CTMessage<K>, Integer> {
    @Override
    public void sendMessages(Vertex<K, CTValue<K>> vertex) {
//        System.out.println("id " + vertex.getId() + " core " + vertex.getValue().getCore() + " coreTime " + vertex.getValue().getCoreTime() + " superstep " + getSuperstepNumber());
//        print as red
        System.out.println("\u001B[31m" + "id " + vertex.getId() + " core " + vertex.getValue().getCore() + " coreTime " + vertex.getValue().getCoreTime() + " superstep " + getSuperstepNumber() + "\u001B[0m");
        ArrayList<K> sendTo = new ArrayList<>();
        if(getSuperstepNumber() == 1){
            ArrayList<NeighborValue<K>> nebrTimeMap = vertex.getValue().getNebrTimeMap();
            for(NeighborValue<K> neighborValue : nebrTimeMap){
                sendMessageTo(neighborValue.getKey(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
                sendTo.add(neighborValue.getKey());
            }
            sendTo.sort(Comparator.naturalOrder());
            System.out.println("\u001B[31m" + "send to: \n" + sendTo + "\u001B[0m");
            return;
        }
        ArrayList<Boolean> updateAt = vertex.getValue().diffCoreTime();
        if(updateAt.contains(true)){
            ArrayList<NeighborValue<K>> nebrTimeMap = vertex.getValue().getNebrTimeMap();
            for(NeighborValue<K> neighborValue : nebrTimeMap){
                boolean send = false;
                int minCore = Math.min(vertex.getValue().getCore(), neighborValue.getCore());
                for(int i = 0; i < minCore; i++){
                    if(vertex.getValue().getCoreTime(i) > neighborValue.getTime()
                     && vertex.getValue().getCoreTime(i) > neighborValue.getCoreTime(i)
                    ) {
                        sendTo.add(neighborValue.getKey());
                        sendMessageTo(neighborValue.getKey(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
                        send = true;
                        break;
                    }
                }
                if(!send){
                    System.out.println("asd");
                }
            }
            sendTo.sort(Comparator.naturalOrder());
            System.out.println("\u001B[31m" + "send to: \n" + sendTo + "\u001B[0m");
        }
    }
}
