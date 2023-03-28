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
        System.out.println("curr step: " + getSuperstepNumber());
        HashSet<K> visited = new HashSet<>();
        for(Edge<K, Integer> edge : getEdges()){
            if(!visited.contains(edge.getTarget())){
                visited.add(edge.getTarget());
                sendMessageTo(edge.getTarget(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
            }
        }
    }
}
