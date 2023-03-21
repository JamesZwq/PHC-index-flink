package main.PHCIndex.PHCVertex;

import java.util.ArrayList;
import java.util.List;

public class NeighborsValue {
    private int core;
    private int CTN;
    private int minTime;

    private List<Integer> coreTimes;

    public NeighborsValue(int core, int CTN) {
        this.core = core;
        this.CTN = CTN;
        coreTimes = new ArrayList<>();
        for (int i = 0; i <= core; i++) {
            coreTimes.add(Integer.MAX_VALUE);
        }
    }

    public NeighborsValue(int core, int CTN, int minTime) {
        this.core = core;
        this.CTN = CTN;
        this.minTime = minTime;
        coreTimes = new ArrayList<>();
        for (int i = 0; i <= core; i++) {
            coreTimes.add(Integer.MAX_VALUE);
        }
    }

    public void setCoreTimes(List<Integer> coreTimes) {
        this.coreTimes = coreTimes;
    }

    public void setCoreTimes(int core, int time) {
        this.coreTimes.set(core, time);
    }

    public void setCTN(int CTN) {
        this.CTN = CTN;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public int getCore() {
        return core;
    }

    public int getCTN() {
        return CTN;
    }

    /**
     * Decrease the CTN and return true if CTN > 0
     * @return true if CTN > 0
     */
    public boolean decreaseCTN(){
        CTN--;
        return CTN > 0;
    }

    public void increaseCTN(){
        CTN++;
    }

    public void setCTNtoZero(){
        CTN = 0;
    }

    public int getMinTime() {
        return minTime;
    }

    @Override
    public String toString() {
        return "NeighborsValue{" +
                "core=" + core +
                ", CTN=" + CTN +
                '}';
    }
}
