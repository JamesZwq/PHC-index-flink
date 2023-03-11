package main.PHCIndex;

import java.util.ArrayList;
import java.util.List;

public class NeighborsValue {
    private int core;
    private int CTN;
    private final List<Integer> coreTimeNb;

    public NeighborsValue(int core, int CTN) {
        this.core = core;
        this.CTN = CTN;
        coreTimeNb = new ArrayList<>();
    }

    public NeighborsValue(int core, int CTN, List<Integer> timeStamps) {
        this.core = core;
        this.CTN = CTN;
        coreTimeNb = new ArrayList<>(timeStamps);
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

    public List<Integer> getCoreTimeNb() {
        return coreTimeNb;
    }

    @Override
    public String toString() {
        return "NeighborsValue{" +
                "core=" + core +
                ", CTN=" + CTN +
                ", coreTimeNb=" + coreTimeNb +
                '}';
    }
}
