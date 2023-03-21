package main.PHCIndex.CoreTime;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class CTvalue<K> {
    private int core;
    /**
     * coreTime:
     * Tuple2<K,Integer>
     * K. the first element is the neighbor
     * Integer. the second element is the time
     */
    private ArrayList<Tuple2<K,Integer>> nebrTimeMap;
    private final ArrayList<Integer> coreTime;
    private final HashMap<K, Tuple2<Integer, ArrayList<Integer>>> neighbors;

    public CTvalue(int core, ArrayList<Tuple2<K,Integer>> nebrTimeMap) {
        this.core = core;
        this.nebrTimeMap = nebrTimeMap.stream().sorted(Comparator.comparing(o -> o.f1)).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        this.neighbors = new HashMap<>();
        this.coreTime = new ArrayList<>();
        int maxTime = this.nebrTimeMap.get(this.nebrTimeMap.size() - 1).f1;
        for(int i = 0; i < this.core; i++){
            this.coreTime.add(maxTime);
        }
    }

    public ArrayList<Integer> getCoreTime() {
        return coreTime;
    }


    public void setCoreTime(int k, int time) {
        this.coreTime.set(k, time);
    }

    public CTvalue(CTvalue<K> c) {
        this.core = c.getCore();
        this.nebrTimeMap = c.getNebrTimeMap();
        this.neighbors = new HashMap<>(c.getNeighbors());
        this.coreTime = c.getCoreTimeList();
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public HashMap<K, Tuple2<Integer, ArrayList<Integer>>> getNeighbors() {
        return neighbors;
    }

    public void setNeighbor(K neighbor, int core, ArrayList<Integer> coreTime) {
        this.neighbors.put(neighbor, new Tuple2<>(core, coreTime));
    }

    public ArrayList<Tuple2<K,Integer>> getNebrTimeMap() {
        return nebrTimeMap;
    }

    public ArrayList<Integer> getCoreTimeList() {
        ArrayList<Integer> coreTimeList = new ArrayList<>();
        for(Tuple2<K,Integer> t : nebrTimeMap){
            coreTimeList.add(t.f1);
        }
        return coreTimeList;
    }

    public void setNebrTimeMap(ArrayList<Tuple2<K,Integer>> nebrTimeMap) {
        this.nebrTimeMap = nebrTimeMap;
    }

//    public void setCoreTime(int k, int time, K neighbor
//        this.coreTime.add(new Tuple2<>(neighbor, time));
//    }

    @Override
    public String toString() {
        return "CTvalue{" +
                "core=" + core +
                ", coreTime=" + nebrTimeMap +
                ", neighbors=" + neighbors +
                '}';
    }
}
