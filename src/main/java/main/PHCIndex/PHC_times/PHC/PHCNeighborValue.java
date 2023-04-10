package main.PHCIndex.PHC_times.PHC;

import java.util.ArrayList;
import java.util.Objects;

public class PHCNeighborValue<K> {
    private final K Id;
    private final ArrayList<Integer> edgeTimes;
    private CoreTimes coreTimes;

    private int core;

    public PHCNeighborValue(K id, ArrayList<Integer> edgeTimes, ArrayList<Integer> coreTimes) {
        this.Id = id;
        this.edgeTimes = edgeTimes;
        this.edgeTimes.sort(Integer::compareTo);
        this.coreTimes = new CoreTimes(new ArrayList<>(coreTimes));
        this.core = coreTimes.size();
    }

    public int getTimeAfter(int time) {
        for (Integer edgeTime : edgeTimes) {
            if (edgeTime >= time) {
                return edgeTime;
            }
        }
        return Integer.MAX_VALUE;
    }

    public int getMaxEdgeTime() {
        return edgeTimes.get(edgeTimes.size() - 1);
    }

    public CoreTimes getCoreTime() {
        return coreTimes;
    }

    public void setCoreTimes(CoreTimes coreTimes) {
        this.coreTimes = new CoreTimes(coreTimes);
    }

    public K getId() {
        return Id;
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PHCNeighborValue<?> that = (PHCNeighborValue<?>) o;
        return core == that.core && Objects.equals(Id, that.Id) && Objects.equals(edgeTimes, that.edgeTimes) && Objects.equals(coreTimes, that.coreTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Id, edgeTimes, coreTimes, core);
    }

    @Override
    public String toString() {
        return "PHCNeighborValue{" +
                "edgeTimes=" + edgeTimes +
                ", coreTimes=" + coreTimes +
                '}';
    }
}
