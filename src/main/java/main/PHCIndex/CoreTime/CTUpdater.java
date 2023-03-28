package main.PHCIndex.CoreTime;

import main.PHCIndex.CoreDecomposition.CDVertexValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public class CTUpdater<K> extends GatherFunction<K, CTvalue<K>, CTMessage<K>> {

    @Override
    public void updateVertex(Vertex<K, CTvalue<K>> vertex, MessageIterator<CTMessage<K>> inMessages) throws Exception {
        CTvalue<K> value = new CTvalue<>(vertex.getValue());
        for (CTMessage<K> message : inMessages) {
            value.addNebrTimeMap(message.getSource(), message.getCore(), message.getCoreTime());
        }
        ArrayList<NeighborValue<K>> rec = value.getNebrTimeMap().stream().sorted(Comparator.comparingInt(NeighborValue::getTime)).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        int maxTime = value.getCoreTime(value.getCore()-1);
        for (int time = maxTime; time >= 0; time--) {
            ArrayList<Integer> cnt = new ArrayList<>();
            for(int i = 0; i < value.getCore(); i++){
                cnt.add(0);
            }
            for (NeighborValue<K> neighbor1 : value.getNebrTimeMap()) {
                if(neighbor1.getTime() <= time){
                    int min = Math.min(neighbor1.getCoreInTime(time), value.getCoreInTime(time));
                    cnt.set(min-1, cnt.get(min-1)+1);
                }
            }
            int cd = 0;
            for(int i = value.getCore()-1; i >= 0; i--){
                cd += cnt.get(i);
                if(i <= cd){
                    value.setCoreTime(i, time);
                    break;
                }
            }
//            if (changedK != value.getCore()) {
//                ArrayList<Integer> T = new ArrayList<>();
//                HashSet<K> visited = new HashSet<>();
//                int ub = 0;
//                for (NeighborValue<K> neighbor1 : rec) {
//                    if(neighbor1.getTime() <= time && neighbor1.getCoreInTime(time) >= changedK && !visited.contains(neighbor1.getKey())){
//                        visited.add(neighbor1.getKey());
//                        T.add(neighbor1.getTime());
//                        ub = Math.max(ub, neighbor1.getTime());
//                    }
//            }
        }

        if (!value.equals(vertex.getValue())) {
            System.out.println("curr id: " + vertex.getId() + " curr step: " + getSuperstepNumber());
            System.out.println("old value: " + vertex.getValue());
            System.out.println("new value: " + value);
            setNewVertexValue(value);
        }
    }
}
