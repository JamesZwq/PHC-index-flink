package main.PHCIndex.PHC_times.PHC;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;

public class PHCMessager<K> extends ScatterFunction<K, PHCValue<K>, PHCMessage<K>, Integer> {
    int time;

    public PHCMessager(int time) {
        this.time = time;
    }

    @Override
    public void sendMessages(Vertex<K, PHCValue<K>> vertex) {
//        for (Edge<K, Integer> edge : getEdges()) {
//            if(edge.getValue() < time) continue;
//            if (edge.getSource().equals(vertex.getId())) {
//                sendMessageTo(edge.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCoreTime()));
//            }
//        }
        ArrayList<Boolean> updatedAt = vertex.getValue().OldCT_CTD_diff();
        for(PHCNeighborValue<K> neighborValue : vertex.getValue().getNeighbors()){
            if(neighborValue.getMaxEdgeTime() < time) continue;
            sendMessageTo(neighborValue.getId(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCoreTime()));
        }
    }
}
