package main.PHCIndex.CoreTime;

import java.util.ArrayList;

public class CTMessage<K> {
    private final K source;
    private final int core;
    private final ArrayList<Integer> coreTime;

    public CTMessage(K neighbor, int core, ArrayList<Integer> coreTime) {
        this.source = neighbor;
        this.core = core;
        this.coreTime = coreTime;
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
}
