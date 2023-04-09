package main.PHCIndex.PHC_times.PHC;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Objects;

public class PHCValue<K> {
    private final ArrayList<PHCNeighborValue<K>> neighbors;
    private final int maxTime;
    private final ArrayList<ArrayList<Tuple2<Integer, Integer>>> PHC_Index;
    private int core;
    private int newCore;
    private final CoreTimes coreTime;
    private CoreTimes oldCoreTimes;
    private boolean isUpdated = false;

    public PHCValue(int core, ArrayList<Tuple3<K, ArrayList<Integer>, ArrayList<Integer>>> neighbors, ArrayList<Integer> coreTime, int maxTime) {
        this.core = core;
        this.newCore = core;
        this.neighbors = new ArrayList<>();
        for (Tuple3<K, ArrayList<Integer>, ArrayList<Integer>> neighbor : neighbors) {
            this.neighbors.add(new PHCNeighborValue<>(neighbor.f0, neighbor.f1, neighbor.f2));
        }
        this.PHC_Index = new ArrayList<>();
        this.maxTime = maxTime;
        this.coreTime = new CoreTimes(coreTime);
        for (int i = 0; i < core; i++) {
            PHC_Index.add(new ArrayList<>());
            PHC_Index.get(i).add(new Tuple2<>(1, coreTime.get(i)));
        }
        ArrayList<Integer> ot = new ArrayList<>();
        for (int i = 0; i < core; i++) {
            ot.add(0);
        }
        this.oldCoreTimes = new CoreTimes(ot);
    }

    public PHCValue(PHCValue<K> v) {
        this.core = v.core;
        this.neighbors = new ArrayList<>(v.neighbors);
        this.coreTime = new CoreTimes(v.coreTime);
        this.maxTime = v.maxTime;
        this.PHC_Index = new ArrayList<>(v.PHC_Index);
        this.oldCoreTimes = new CoreTimes(v.oldCoreTimes);

    }

    public CoreTimes getOldCoreTimes() {
        return oldCoreTimes;
    }

    public void setOldCTtoCT() {
        this.oldCoreTimes = new CoreTimes(this.coreTime);
    }

    public ArrayList<PHCNeighborValue<K>> getNeighbors() {
        return neighbors;
    }

    public PHCNeighborValue<K> getNeighbors(K k) {
        ArrayList<PHCNeighborValue<K>> neighbors = new ArrayList<>();
        for (PHCNeighborValue<K> neighbor : this.neighbors) {
            if (neighbor.getId().equals(k)) {
                return neighbor;
            }
        }
        return null;
    }

    public ArrayList<Boolean> OldCT_CTD_diff() {
        ArrayList<Boolean> diff = new ArrayList<>();
        for (int i = 0; i < core; i++) {
            diff.add(!Objects.equals(oldCoreTimes.get(i), coreTime.get(i)));
        }
        return diff;
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public int getNewCore() {
        return newCore;
    }

    public void setNewCore(int newCore) {
        this.newCore = newCore;
    }

    public CoreTimes getCoreTime() {
        return coreTime;
    }

    public int getMaxTime() {
        return maxTime;
    }

    public void addNeighbor(K key, CoreTimes coreTime, int core) {
        PHCNeighborValue<K> neighbor = neighbors.stream().filter(n -> n.getId().equals(key)).findFirst().get();
        neighbors.removeIf(n -> n.getId().equals(key));
        neighbor.setCoreTimes(coreTime);
        neighbor.setCore(core);
        neighbors.add(neighbor);
    }

    public void reduceCore() {
        this.newCore--;
    }

    public void addPHC_Index(int k, int from, int to) {
//        PHC_Index.get(k).add(new Tuple2<>(from, to));
        if (PHC_Index.get(k).get(PHC_Index.get(k).size() - 1).f0 == from) {
            PHC_Index.get(k).get(PHC_Index.get(k).size() - 1).f1 = to;
        } else {
            PHC_Index.get(k).add(new Tuple2<>(from, to));
        }
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    public void setUpdated(boolean updated) {
        isUpdated = updated;
    }

    @Override
    public String toString() {
        return PHC_IndexToString();
    }

    public String PHC_IndexToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        for (int i = 1; i < PHC_Index.size(); i++) {
            sb.append("k = ").append(i + 1).append(" : ");
            for (Tuple2<Integer, Integer> t : PHC_Index.get(i)) {
                sb.append(t).append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
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
