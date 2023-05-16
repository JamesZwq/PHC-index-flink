package main.PHCIndex.CoreDecomposition;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CDVertexValue<K> {
    private final HashMap<K, Integer> neighbors;
    private int core;
    private final int[] cnts;
    private int cnt;
    private int oldCore;
    final private int degree;

    public int getDegree() {
        return degree;
    }

    public CDVertexValue(int core, HashMap<K, Integer> neighbors) {
        this.core = core;
        this.degree = core;
        this.oldCore = core+1;
        this.cnts = new int[core+1];
        this.neighbors = neighbors;
        for (int i = 0; i <= this.core; i++) {
            this.cnts[i] = 0;
        }
        for (Integer nei : new ArrayList<>(neighbors.values())) {
            int k = Math.min(nei, core);
            this.cnts[k] = this.cnts[k] + 1;
        }
        this.cnt = this.cnts[core];

    }

    public CDVertexValue(CDVertexValue<K> c) {
        this.core = c.getCore();
        this.degree = c.getDegree();
        this.oldCore = c.getOldCore();
        this.cnts = c.getCnts();
        this.neighbors = new HashMap<>(c.getNeighbors());
        this.cnt = c.getCnt();
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
        return core == that.core && cnts == that.cnts && oldCore == that.oldCore && Objects.equals(neighbors, that.neighbors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(core, cnts, oldCore, neighbors);
    }


    @Override
    public String toString() {
        return "CDVertexValue{" +
                "core=" + core +
                ", oldCore=" + oldCore +
                ", cnts=" + cnts +
                ", cnt=" + cnt +
                ", neighbors=" + neighbors +
                '}';
    }
}
