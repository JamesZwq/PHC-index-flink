package main.PHCIndex.CoreDecomposition_base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CDVertexValue<K> {
    private final HashMap<K, Integer> neighbors;
    private int core;
    private int oldCore;

    public CDVertexValue(int core, HashMap<K, Integer> neighbors) {
        this.core = core;
        this.oldCore = core+1;
        this.neighbors = neighbors;
    }

    public CDVertexValue(CDVertexValue<K> c) {
        this.core = c.getCore();
        this.oldCore = c.getOldCore();
        this.neighbors = new HashMap<>(c.getNeighbors());
    }

    public int getCore() {
        return core;
    }
    public void setCore(int core) {
        this.core = core;
    }
    public HashMap<K, Integer> getNeighbors() {
        return neighbors;
    }

    public void setNeighbor(K neighbor, int core) {
        this.neighbors.put(neighbor, core);
    }

    public int getOldCore() {
        return oldCore;
    }

    public void setOldCore(int oldCore) {
        this.oldCore = oldCore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CDVertexValue<?> that = (CDVertexValue<?>) o;
        return core == that.core && oldCore == that.oldCore && Objects.equals(neighbors, that.neighbors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(core, oldCore, neighbors);
    }


    @Override
    public String toString() {
        return "CDVertexValue{" +
                "core=" + core +
                ", oldCore=" + oldCore +
                ", neighbors=" + neighbors +
                '}';
    }
}
