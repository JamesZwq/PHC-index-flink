package main.PHCIndex.CoreDecomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CDVertexValue<K> {
    // <neighbor, <core, cnt>>
    private final HashMap<K, Integer> neighbors;
    private int core;
    private final ArrayList<Integer> cnts;
    private int cnt;
    private int oldCore;

    public CDVertexValue(int core, HashMap<K, Integer> neighbors) {
        this.core = core;
        this.oldCore = core+1;
        this.cnts = new ArrayList<>();
        this.neighbors = neighbors;
        for (int i = 0; i <= this.core; i++) {
            cnts.add(0);
        }
        for (K nei : neighbors.keySet()) {
            int k = neighbors.get(nei);//old core
            k = Math.min(k, core);
            cnts.set(k, cnts.get(k) + 1);
        }
        this.cnt = 0;
    }

    public CDVertexValue(CDVertexValue<K> c) {
        this.core = c.getCore();
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

    public ArrayList<Integer> getCnts() {
        return cnts;
    }

    public void increaseCnt(int k) {
        k = Math.min(k, cnts.size()-1);
        this.cnts.set(k, this.cnts.get(k) + 1);
    }
    public void reduceCnt(int k) {
        k = Math.min(k, cnts.size()-1);
        this.cnts.set(k, this.cnts.get(k) - 1);
    }

    public void setCnts(int k, int val) {
        this.cnts.set(k, val);
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
                "neighbors=" + neighbors +
                ", core=" + core +
                ", cnts=" + cnts +
                ", cnt=" + cnt +
                ", oldCore=" + oldCore +
                '}';
    }
}
