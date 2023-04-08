package main.PHCIndex.PHC_times.PHC;

import java.util.ArrayList;

public class CoreTimes {
    private final ArrayList<Integer> coreTime;

    public CoreTimes(ArrayList<Integer> coreTime) {
        this.coreTime = coreTime;
    }

    public CoreTimes(CoreTimes coreTimes) {
        this.coreTime = new ArrayList<>(coreTimes.get());
    }

    public ArrayList<Integer> get() {
        return coreTime;
    }

    public Integer get(int k) {
        ArrayList<Integer> integers = coreTime;
        if (integers.size() > k) {
            return integers.get(k);
        }
        return Integer.MAX_VALUE;
    }

    public void set(int k, int value) {
        ArrayList<Integer> integers = coreTime;
        if (integers.size() > k) {
            integers.set(k, value);
        }
    }

    @Override
    public String toString() {
        return "CoreTimes{" +
                "coreTime=" + coreTime +
                '}';
    }
}
