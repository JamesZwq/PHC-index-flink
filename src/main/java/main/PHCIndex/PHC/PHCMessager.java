package main.PHCIndex.PHC;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;

public class PHCMessager<K> extends ScatterFunction<K, PHCValue<K>, PHCMessage<K>, Integer> {
    @Override
    public void sendMessages(Vertex<K, PHCValue<K>> vertex) throws Exception {
        System.out.println("superstep: " + getSuperstepNumber());
        for(Edge<K, Integer> edge : getEdges()) {
            if (edge.getSource().equals(vertex.getId())) {
                sendMessageTo(edge.getTarget(), new PHCMessage<>(vertex.getId(), vertex.getValue().getCoreTime()));
            }
        }
    }
}
