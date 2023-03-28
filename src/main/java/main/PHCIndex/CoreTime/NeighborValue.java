package main.PHCIndex.CoreTime;

import java.util.ArrayList;
import java.util.Objects;

public class NeighborValue<K> {
    private final ArrayList<Integer> coreTime;
    private final K key;
    private final Integer time;
    private final Integer core;
    /**
     * @param key is the neighbor
     * @param time is the last edge time
     * @param core is the core number
     */
    public NeighborValue(K key, Integer time, Integer core) {
        this.key = key;
        this.time = time;
        this.core = core;
        this.coreTime = new ArrayList<>();
    }

    public NeighborValue(K key, Integer time, Integer core, ArrayList<Integer> coreTime) {
        this.key = key;
        this.time = time;
        this.core = core;
        this.coreTime = new ArrayList<>(coreTime);
    }

    public NeighborValue(NeighborValue<K> n) {
        this.key = n.getKey();
        this.time = n.getTime();
        this.core = n.getCore();
        this.coreTime = new ArrayList<>(n.getCoreTime());
    }

    public ArrayList<Integer> getCoreTime() {
        return coreTime;
    }

    public K getKey(){
        return this.key;
    }
    public int getTime(){
        return this.time;
    }
    public int getCore(){
        return this.core;
    }

    public ArrayList<Integer> getCoreTime(int k) {
        return coreTime;
    }

    public Integer getCoreInTime(int time) {
        for(int i = core-1; i >= 0; i--){
            if(coreTime.get(i) <= time){
                return i+1;
            }
        }
        return core;
    }

    @Override
    public String toString() {
        return "{" +
                "key=" + key +
                ", time=" + time +
                ", core=" + core +
                ", coreTime=" + coreTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NeighborValue<?> that = (NeighborValue<?>) o;
        return Objects.equals(coreTime, that.coreTime) && Objects.equals(key, that.key) && Objects.equals(time, that.time) && Objects.equals(core, that.core);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coreTime, key, time, core);
    }
}
