package main.PHCIndex.CoreTime;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.HashSet;

public class CTMessager<K> extends ScatterFunction<K, CTValue<K>, CTMessage<K>, Integer> {
    @Override
    public void sendMessages(Vertex<K, CTValue<K>> vertex) throws Exception {
        HashSet<K> visited = new HashSet<>();
        for(Edge<K, Integer> edge : getEdges()){
            if(!visited.contains(edge.getTarget())){
                visited.add(edge.getTarget());
                sendMessageTo(edge.getTarget(), new CTMessage<>(vertex.getId(), vertex.getValue().getCore(), vertex.getValue().getCoreTime()));
            }
        }
    }
}
