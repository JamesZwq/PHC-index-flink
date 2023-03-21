package main.PHCIndex.InitCT;


/**
 * @param <K> The type of the vertex key.
 */
public class InitCTMessage<K> {
    private final int core;
    private final K u;
    private final boolean shouldUpdate;

    private final int MinTime;

    public InitCTMessage(K u, int core, int minTime,boolean shouldUpdate) {
        this.core = core;
        this.u = u;
        this.shouldUpdate = shouldUpdate;
        this.MinTime = minTime;
    }
    public boolean isShouldUpdate() {
        return shouldUpdate;
    }

    public int getMinTime() {
        return MinTime;
    }

    public int getCore() {
        return core;
    }

    public K getSource() {
        return u;
    }
}
