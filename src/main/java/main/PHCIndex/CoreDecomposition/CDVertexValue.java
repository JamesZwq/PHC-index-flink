package main.PHCIndex.CoreDecomposition;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Objects;

public class CDVertexValue<K> {
    private int core;
    private int cnt;

    private int oldCore;
    // <neighbor, <core, cnt>>
    private final HashMap<K, Tuple2<Integer, Integer>> neighbors;

    public CDVertexValue(int core) {
        this.core = core;
        this.oldCore = core;
        this.cnt = 0;
        this.neighbors = new HashMap<>();
    }

    public CDVertexValue(CDVertexValue<K> c) {
        this.core = c.getCore();
        this.oldCore = c.getOldCore();
        this.cnt = c.getCnt();
        this.neighbors = new HashMap<>(c.getNeighbors());
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public HashMap<K, Tuple2<Integer, Integer>> getNeighbors() {
        return neighbors;
    }

    public void setNeighbor(K neighbor, int core, int cnt) {
        this.neighbors.put(neighbor, new Tuple2<>(core, cnt));
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
        return core == that.core && cnt == that.cnt && oldCore == that.oldCore && Objects.equals(neighbors, that.neighbors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(core, cnt, oldCore, neighbors);
    }


    @Override
    public String toString() {
        return "CDVertexValue{" +
                "core=" + core +
                ", cnt=" + cnt +
                ", oldCore=" + oldCore +
                ", neighbors=" + neighbors +
                '}';
    }
}
