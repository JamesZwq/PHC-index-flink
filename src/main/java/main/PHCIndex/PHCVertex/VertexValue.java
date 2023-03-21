package main.PHCIndex.PHCVertex;

import java.util.*;

public class VertexValue<K> {
    private final HashMap<K,NeighborsValue> neighbors;
    private int core;
    private int oldCore;
    private final List<Integer> coreTime;
//    private final List<List<Tuple2<Integer,Integer>>> coreTime;

    private boolean calculated;

    public VertexValue(int k, HashMap<K,NeighborsValue> neighbors) {
        this.core = k;
        this.oldCore = k;
        this.neighbors = neighbors;
        this.calculated = false;
        this.coreTime = new ArrayList<>();
        for (int i = 0; i <= k; i++) {
            coreTime.add(Integer.MAX_VALUE);
//            coreTime.add(new ArrayList<>());
        }
    }

    public VertexValue(VertexValue<K> v) {
        this.core = v.core;
        this.oldCore = v.oldCore;
        this.neighbors = new HashMap<>(v.neighbors);
        this.coreTime = new ArrayList<>(v.coreTime);
        this.calculated = v.calculated;
    }

    public List<Integer> getCoreTimes() {
        return coreTime;
    }

    public void setCalculatedCoreCN(){
        calculated = true;
    }

    public void setCalculatedCoreCNFalse(){
        calculated = false;
    }

    public boolean isCalculated() {
        return calculated;
    }

    public HashMap<K, NeighborsValue> getNeighbors() {
        return neighbors;
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public void decreaseNeighbors(K v){
        if (!neighbors.get(v).decreaseCTN()){
            neighbors.remove(v);
        }
    }

    public void resetCoreTimeNeighbors(){
        for (NeighborsValue n : neighbors.values()) {
            n.setCTN(0);
        }
    }


    public void addCoreTime(int k, int te){

        coreTime.set(k,te);
//        coreTime.get(k).add(new Tuple2<>(ts,te));
    }

    public int getOldCore() {
        return oldCore;
    }

    public void setOldCore(int oldCore) {
        this.oldCore = oldCore;
    }

    public void setCalculated(boolean calculated) {
        this.calculated = calculated;
    }

    public int getCTNSize(){
        int size = 0;
        for (NeighborsValue n : neighbors.values()) {
            if(n.getCTN() > 0)
                size++;
        }
        return size;
    }

    public void insertNeighbors(K v, int core, int minTime){
        neighbors.put(v,new NeighborsValue(core,0,minTime));
    }

    @Override
    public String toString() {
        return "VertexValue{" +
                ", core=" + core +
                '}';
    }


//    @Override
//    public String toString() {
//        return "VertexValue{" +
//                "coreTime=" + coreTime +
//                "}\n";
//    }

    public String neighborsToString(){
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K,NeighborsValue> n : neighbors.entrySet()) {
            sb.append(n.getValue().toString()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexValue<?> that = (VertexValue<?>) o;
        return core == that.core && calculated == that.calculated && Objects.equals(neighbors, that.neighbors) && Objects.equals(coreTime, that.coreTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbors, core, coreTime, calculated);
    }
}
