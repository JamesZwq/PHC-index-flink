package main.PHCIndex;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class VertexValue<K> {
    private final HashMap<K,NeighborsValue> neighbors;
    private int core;

    public VertexValue(int k, HashMap<K,NeighborsValue> neighbors) {
        this.core = k;
        this.neighbors = neighbors;
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    @Override
    public String toString() {
        return "VertexValue{" +
                "neighbors=" + neighbors +
                ", core=" + core +
                '}';
    }
}
