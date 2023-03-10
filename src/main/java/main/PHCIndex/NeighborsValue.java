package main.PHCIndex;

import java.util.HashMap;

public class NeighborsValue {
    private int core;
    private int coreTime;
    private int coreTimeNb;

    public NeighborsValue(int core, int coreTime) {
        this.core = core;
        this.coreTime = coreTime;
        coreTimeNb = 1;
    }

    public NeighborsValue(int core, int coreTime, int coreTimeNb) {
        this.core = core;
        this.coreTime = coreTime;
        this.coreTimeNb = coreTimeNb;
    }

    public void addCoreTimeNb(){
        coreTimeNb++;
    }

    public int getCore() {
        return core;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public int getCoreTime() {
        return coreTime;
    }

    public void setCoreTime(int coreTime) {
        this.coreTime = coreTime;
    }

    @Override
    public String toString() {
        return "{" +
                "core=" + core +
                ", coreTime=" + coreTime +
                '}';
    }
}
