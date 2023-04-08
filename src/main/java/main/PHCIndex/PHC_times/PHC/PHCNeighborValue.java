package main.PHCIndex.PHC_times.PHC;

import java.util.ArrayList;
import java.util.Objects;

public class PHCNeighborValue<K> {
    private final K Id;
    private final ArrayList<Integer> edgeTimes;
    private CoreTimes coreTimes;

    public PHCNeighborValue(K id, ArrayList<Integer> edgeTimes, ArrayList<Integer> coreTimes) {
        this.Id = id;
        this.edgeTimes = edgeTimes;
        this.edgeTimes.sort(Integer::compareTo);
        this.coreTimes = new CoreTimes(new ArrayList<>(coreTimes));
    }

    public int getTimeAfter(int time) {
        for(int i = 0; i < edgeTimes.size(); i++) {
            if (edgeTimes.get(i) >= time) {
                return edgeTimes.get(i);
            }
        }
        return Integer.MAX_VALUE;
    }

    public int getMaxEdgeTime() {
        return edgeTimes.get(edgeTimes.size() - 1);
    }

    public CoreTimes getCoreTimes() {
        return coreTimes;
    }

    public void setCoreTimes(CoreTimes coreTimes) {
        this.coreTimes = new CoreTimes(coreTimes);
    }

    public K getId() {
        return Id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PHCNeighborValue<K> that = (PHCNeighborValue<K>) o;
        return Objects.equals(edgeTimes, that.edgeTimes) && Objects.equals(coreTimes, that.coreTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeTimes, coreTimes);
    }

    @Override
    public String toString() {
        return "PHCNeighborValue{" +
                "edgeTimes=" + edgeTimes +
                ", coreTimes=" + coreTimes +
                '}';
    }
}
