package main.PHCIndex.CoreDecomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CDVertexValue<K> {
    private final HashMap<K, Integer> neighbors;
    private int core;
    private final int[] cnts;
    private int cnt;
    private boolean changed;
    final private int degree;
    public int numMsg = 0;

    public int completeAt = 0;

    public int getDegree() {
        return degree;
    }

    public CDVertexValue(int core, HashMap<K, Integer> neighbors) {
        this.core = core;
        this.degree = core;
        this.changed = true;
        this.cnts = new int[core+1];
        this.neighbors = neighbors;
        for (Integer nei : new ArrayList<>(neighbors.values())) {
            int k = Math.min(nei, core);
            this.cnts[k] = this.cnts[k] + 1;
        }
        this.cnt = this.cnts[core];
    }

    public int getCore() {
        return core;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public int[] getCnts() {
        return cnts;
    }
    public void setCnts(int k, int val) {
        this.cnts[k] = val;
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
                ", cnts=" + cnts +
                ", cnt=" + cnt +
                ", neighbors=" + neighbors +
                '}';
    }
}
