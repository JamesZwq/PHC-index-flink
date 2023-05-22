package main.PHCIndex.CoreDecomposition_base;

import java.util.HashMap;
import java.util.Objects;

public class CDVertexValue<K> {
    private final HashMap<K, Integer> neighbors;
    private int core;
    private boolean changed;
    public int numMsg = 0;

    public CDVertexValue(int core, HashMap<K, Integer> neighbors) {
        this.core = core;
        this.changed = true;
        this.neighbors = neighbors;
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

    public boolean getChanged() {
        return changed;
    }

    public void setChanged() {
        this.changed = true;
    }

    public void setUnchanged() {
        this.changed = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CDVertexValue<?> that = (CDVertexValue<?>) o;
        return core == that.core && changed == that.changed && Objects.equals(neighbors, that.neighbors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(core, changed, neighbors);
    }


    @Override
    public String toString() {
        return "CDVertexValue{" +
                "core=" + core +
                ", oldCore=" + changed +
                ", neighbors=" + neighbors +
                '}';
    }
}
