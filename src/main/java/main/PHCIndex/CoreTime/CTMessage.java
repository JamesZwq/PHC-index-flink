package main.PHCIndex.CoreTime;

import java.util.ArrayList;

public class CTMessage<K> {
    private final K source;
    private final int core;
    private final ArrayList<Integer> coreTime;
    private final int time;

    public CTMessage(K neighbor, int core, ArrayList<Integer> coreTime, int time) {
        this.source = neighbor;
        this.core = core;
        this.coreTime = coreTime;
        this.time = time;
    }

    public K getSource() {
        return source;
    }

    public int getCore() {
        return core;
    }

    public ArrayList<Integer> getCoreTime() {
        return coreTime;
    }

    public int getTime() {
        return time;
    }
}
