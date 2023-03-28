package main.PHCIndex.CoreTime;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;

public class CTvalue<K> {
    private int core;
    private final ArrayList<NeighborValue<K>> nebrTimeMap;
    private final ArrayList<Integer> coreTime;

    public CTvalue(int core,ArrayList<NeighborValue<K>> nebrTimeMap) {
        this.core = core;
        this.nebrTimeMap = nebrTimeMap.stream().sorted(Comparator.comparing(o -> o.getTime())).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        this.coreTime = new ArrayList<>();
        for(int i = 0; i < this.core; i++){
            this.coreTime.add(nebrTimeMap.get(i).getTime());
        }
    }

    public CTvalue(CTvalue<K> c) {
        this.core = c.getCore();
        this.nebrTimeMap = new ArrayList<>();
        for (NeighborValue<K> n : c.getNebrTimeMap()) {
            this.nebrTimeMap.add(new NeighborValue<>(n));
        }
        this.coreTime = new ArrayList<>(c.getCoreTime());
    }

    public ArrayList<Integer> getCoreTime() {
        return coreTime;
    }

    public int getCoreTime(int k) {
        if(k < coreTime.size())
            return coreTime.get(k);
        else
            return Integer.MAX_VALUE;
    }

    public void setCoreTime(int k, int time) {
        coreTime.set(k, time);
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public ArrayList<NeighborValue<K>> getNebrTimeMap() {
        return nebrTimeMap;
    }

    public void addNebrTimeMap(K key, Integer core, ArrayList<Integer> coreTime) {
        NeighborValue<K> curr = nebrTimeMap.stream().filter(t -> t.getKey().equals(key)).findFirst().get();
        int time = curr.getTime();
        nebrTimeMap.removeIf(t -> t.getKey().equals(key));
        nebrTimeMap.add(new NeighborValue<>(key, time, core, new ArrayList<>(coreTime)));
        nebrTimeMap.sort(Comparator.comparing(o -> o.getTime()*-1));
    }


    @Override
    public String toString() {
        return "CTvalue{" +
                "core=" + core +
                " nebrSize=" + nebrTimeMap.stream().filter(t -> t.getCore() >= core).count() +
                ", nebrTimeMap=" + nebrTimeMap +
                ", coreTime=" + coreTime +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CTvalue<?> cTvalue = (CTvalue<?>) o;
        return core == cTvalue.core && Objects.equals(coreTime, cTvalue.coreTime) && Objects.equals(nebrTimeMap, cTvalue.nebrTimeMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(core, nebrTimeMap, coreTime);
    }
}
