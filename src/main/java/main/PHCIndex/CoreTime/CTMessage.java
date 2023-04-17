package main.PHCIndex.CoreTime;

import java.util.ArrayList;
import java.util.List;

public class CTMessage<K> {
    private final K source;
    private final int core;
    private final ArrayList<Integer> coreTime;

    private final List<Boolean> updateAt;

    public CTMessage(K neighbor, int core, ArrayList<Integer> coreTime, List<Boolean> updateAt) {
        this.source = neighbor;
        this.core = core;
        this.coreTime = coreTime;
        this.updateAt = updateAt;
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

    public List<Boolean> getUpdateAt() {

        return updateAt;
    }
}
