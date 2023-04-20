package main.PHCIndex.CoreTime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;

public class CTValue<K> {

    private final ArrayList<NeighborValue<K>> nebrTimeMap;
    private final ArrayList<Integer> coreTime;
    private ArrayList<Integer> oldCoreTime;
    private int core;

    public CTValue(int core, ArrayList<NeighborValue<K>> nebrTimeMap) {
        this.core = core;
        this.nebrTimeMap = nebrTimeMap.stream().sorted(Comparator.comparing(NeighborValue::getTime)).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        this.coreTime = new ArrayList<>();
        for (int i = 0; i < this.core; i++) {
            this.coreTime.add(this.nebrTimeMap.get(i).getTime());
        }
        this.oldCoreTime = new ArrayList<>();
        for (int i = 0; i < this.core; i++) {
            this.oldCoreTime.add(Integer.MAX_VALUE);
        }
    }
    public CTValue(int core, ArrayList<Integer> coreTime, ArrayList<NeighborValue<K>> nebrTimeMap) {
        this.core = core;
        this.nebrTimeMap = nebrTimeMap.stream().sorted(Comparator.comparing(NeighborValue::getTime)).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        this.coreTime = new ArrayList<>(coreTime);
        this.oldCoreTime = new ArrayList<>();
        for (int i = 0; i < this.core; i++) {
            this.oldCoreTime.add(Integer.MAX_VALUE);
        }
    }

    public CTValue(CTValue<K> c) {
        this.core = c.getCore();
        this.nebrTimeMap = new ArrayList<>();
        for (NeighborValue<K> n : c.getNebrTimeMap()) {
            this.nebrTimeMap.add(new NeighborValue<>(n));
        }
        this.coreTime = new ArrayList<>(c.getCoreTime());
        this.oldCoreTime = new ArrayList<>(c.getOldCoreTime());
    }

    public ArrayList<Boolean> diffCoreTime() {
        ArrayList<Boolean> diff = new ArrayList<>();
        for (int i = 0; i < this.core; i++) {
            diff.add(!Objects.equals(this.coreTime.get(i), this.oldCoreTime.get(i)));
        }
        return diff;
    }

    public ArrayList<Integer> getOldCoreTime() {
        return oldCoreTime;
    }

    public void setOldCoreTime(ArrayList<Integer> oldCoreTime) {
        this.oldCoreTime = new ArrayList<>(oldCoreTime);
    }

    public ArrayList<Integer> getCoreTime() {
        return coreTime;
    }

    public Integer getCoreTime(int k) {
        return coreTime.get(k);
    }

    public void setCoreTime(int k, int time) {
//        if (coreTime.get(k) >= time) {
//            CoreTime.runTimes++;
//            System.out.println("runTimes: " + CoreTime.runTimes);
//            return;
//        }
        coreTime.set(k, time);
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
        for (int i = 0; i < this.core; i++) {
            this.coreTime.add(nebrTimeMap.get(i).getTime());
        }
    }

    public ArrayList<NeighborValue<K>> getNebrTimeMap() {
        return nebrTimeMap;
    }

    public void addNebrTimeMap(K key, Integer core, ArrayList<Integer> coreTime) {
        int index = -1;
        for (int i = 0; i < nebrTimeMap.size(); i++) {
            if (nebrTimeMap.get(i).getKey().equals(key)) {
                index = i;
                break;
            }
        }
        NeighborValue<K> curr = nebrTimeMap.get(index);
        int time = curr.getTime();
        nebrTimeMap.set(index, new NeighborValue<>(key, time, core, new ArrayList<>(coreTime)));
    }

    @Override
    public String toString() {
        return coreTime.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CTValue<?> ctValue = (CTValue<?>) o;
        return core == ctValue.core && Objects.equals(nebrTimeMap, ctValue.nebrTimeMap) && Objects.equals(coreTime, ctValue.coreTime) && Objects.equals(oldCoreTime, ctValue.oldCoreTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nebrTimeMap, coreTime, oldCoreTime, core);
    }
}
