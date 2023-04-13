package main.PHCIndex.PHC_times.PHC;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.HashSet;

public class PHCMessager<K> extends ScatterFunction<K, PHCValue<K>, PHCMessage<K>, Integer> {
    int time;

    public PHCMessager(int time) {
        this.time = time;
    }

    @Override
    public void sendMessages(Vertex<K, PHCValue<K>> vertex) {
        if(getSuperstepNumber() == 1){
            ArrayList<Boolean> updatedAt = new ArrayList<>();
            for(int i = 0; i < vertex.getValue().getCore(); i++){
                updatedAt.add(true);
            }
            HashSet<Integer> visited = new HashSet<>();
            for (Edge<K, Integer> edge : getEdges()) {
                if(edge.getValue() != time && edge.getValue() != time-1) continue;
                if(visited.contains(edge.getTarget().hashCode())) continue;
                if (!edge.getSource().equals(vertex.getId())) continue;
                PHCNeighborValue<K> neighbors = vertex.getValue().getNeighbors(edge.getTarget());
                if(vertex.getValue().getCore() < neighbors.getCore()) continue;
                int k = Math.min(vertex.getValue().getCore(), neighbors.getCore());
                visited.add(edge.getTarget().hashCode());
                sendMessageTo(edge.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCoreTime(), updatedAt, vertex.getValue().getCore()));
            }
            return;
        }
        if(!vertex.getValue().isUpdated()) return;
        for(PHCNeighborValue<K> neighborValue : vertex.getValue().getNeighbors()){
            ArrayList<Boolean> updatedAt = vertex.getValue().OldCT_CTD_diff();
            if(neighborValue.getMaxEdgeTime() < time) continue;
            int k = Math.min(vertex.getValue().getCore(), neighborValue.getCore());
            for(int i = 1; i < k; i++){
                if(!updatedAt.get(i)) continue;
                int kt = Math.min(vertex.getValue().getCoreTime().get(i), time);
                int ctNeighbor = neighborValue.getCoreTime().get(i);
                int myTimeAfter = vertex.getValue().getCoreTime().get(i);
                if(!(kt <= ctNeighbor && ctNeighbor < myTimeAfter)) {
                    updatedAt.set(i, false);
                }
            }
            sendMessageTo(neighborValue.getId(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCoreTime(), updatedAt, vertex.getValue().getCore()));
        }
    }
}
