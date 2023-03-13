package main.PHCIndex.PHC;


import java.util.List;
/**
 * @param <K> The type of the vertex key.
 */
public class PHCMessage<K> {
    private final int core;
    private final K u;
    private final boolean shouldUpdate;
    private final List<Integer> CoreTimes;

    public PHCMessage(K u, int core, boolean shouldUpdate, List<Integer> CoreTimes) {
        this.core = core;
        this.u = u;
        this.shouldUpdate = shouldUpdate;
        this.CoreTimes = CoreTimes;
    }
    public boolean isShouldUpdate() {
        return shouldUpdate;
    }

    public int getCore() {
        return core;
    }

    public K getSource() {
        return u;
    }

    public List<Integer> getCoreTimes() {
        return CoreTimes;
    }
}
