package main.PHCIndex.PHC;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PHCNeighborValue<K> {
    private final K Id;
    private final ArrayList<Integer> edgeTimes;
    private CoreTimes coreTimes;

    public PHCNeighborValue(K id, ArrayList<Integer> edgeTimes, ArrayList<ArrayList<Integer>> coreTimes) {
        this.Id = id;
        this.edgeTimes = edgeTimes;
        this.edgeTimes.sort(Integer::compareTo);
        this.coreTimes = new CoreTimes(new ArrayList<>(coreTimes));
    }

    public int getMaxEdgeTime() {
        return edgeTimes.get(edgeTimes.size() - 1);
    }

    public ArrayList<Integer> getEdgeTimes() {
        return edgeTimes;
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
        PHCNeighborValue that = (PHCNeighborValue) o;
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
