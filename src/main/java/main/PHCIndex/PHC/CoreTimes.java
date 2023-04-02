package main.PHCIndex.PHC;

import java.util.ArrayList;
import java.util.Objects;

public class CoreTimes {
    private final ArrayList<ArrayList<Integer>> coreTime;

    public CoreTimes(ArrayList<ArrayList<Integer>> coreTime) {
        this.coreTime = coreTime;
    }

    public CoreTimes(CoreTimes coreTimes) {
        this.coreTime = new ArrayList<>();
        for (ArrayList<Integer> integers : coreTimes.get()) {
            this.coreTime.add(new ArrayList<>(integers));
        }
    }

    public ArrayList<ArrayList<Integer>> get() {
        return coreTime;
    }

    public Integer get(int t, int k) {
        ArrayList<Integer> integers = coreTime.get(t);
        if(integers.size() > k){
            return integers.get(k);
        }
        return Integer.MAX_VALUE;
    }

    public void set(int t, int k, int value) {
        for(int time = t; time < coreTime.size(); time++){
            ArrayList<Integer> integers = coreTime.get(time);
            if(integers.size() > k && integers.get(k) < value){
                integers.set(k, value);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n");
        for (int i = 0; i < coreTime.size(); i++) {
            stringBuilder.append(i).append(": ");
            for (int j = 0; j < coreTime.get(i).size(); j++) {
                stringBuilder.append(coreTime.get(i).get(j)).append(" ");
            }
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
