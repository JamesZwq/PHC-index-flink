package main.PHCIndex.PHC;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Objects;

public class PHCValue<K> {
    private final ArrayList<PHCNeighborValue<K>> neighbors;
    private final int core;
    private final CoreTimes coreTime;

    private final int maxTime;

    public PHCValue(int core, ArrayList<Tuple3<K, ArrayList<Integer>, ArrayList<Integer>>> neighbors, ArrayList<Integer> coreTime, int maxTime) {
        this.core = core;
//        this.neighbors = neighbors;
        this.neighbors = new ArrayList<>();
        for (Tuple3<K, ArrayList<Integer>, ArrayList<Integer>> neighbor : neighbors) {
            ArrayList<ArrayList<Integer>> coreTimeNeighbor = new ArrayList<>();
            for (int time = 0; time < maxTime; time++) {
                coreTimeNeighbor.add(new ArrayList<>(neighbor.f2));
            }
            this.neighbors.add(new PHCNeighborValue<>(neighbor.f0, neighbor.f1, coreTimeNeighbor));
        }
        ArrayList<ArrayList<Integer>> ct = new ArrayList<>();
        this.maxTime = maxTime;
        for (int i = 0; i < maxTime; i++) {
            ct.add(new ArrayList<>(coreTime));
        }
        this.coreTime = new CoreTimes(ct);
    }

    public PHCValue(PHCValue<K> v) {
        this.core = v.core;
        this.neighbors = new ArrayList<>(v.neighbors);
        this.coreTime = new CoreTimes(v.coreTime);
        this.maxTime = v.maxTime;
    }


    public ArrayList<PHCNeighborValue<K>> getNeighbors() {
        return neighbors;
    }

    public int getCore() {
        return core;
    }

    public CoreTimes getCoreTime() {
        return coreTime;
    }

    public int getMaxTime() {
        return maxTime;
    }

    public void addNeighbor(K key, CoreTimes coreTime) {
        PHCNeighborValue<K> neighbor = neighbors.stream().filter(n -> n.getId().equals(key)).findFirst().get();
        neighbors.removeIf(n -> n.getId().equals(key));
        neighbor.setCoreTimes(coreTime);
        neighbors.add(neighbor);
    }

    @Override
    public String toString() {
        return coreTime.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PHCValue<?> that = (PHCValue<?>) o;
        return core == that.core && Objects.equals(neighbors, that.neighbors) && Objects.equals(coreTime, that.coreTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbors, core, coreTime);
    }
}
