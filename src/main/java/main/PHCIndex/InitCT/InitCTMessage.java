package main.PHCIndex.InitCT;


/**
 * @param <K> The type of the vertex key.
 */
public class InitCTMessage<K> {
    private final int core;
    private final K u;
    private final boolean shouldUpdate;

    public InitCTMessage(K u, int core, boolean shouldUpdate) {
        this.core = core;
        this.u = u;
        this.shouldUpdate = shouldUpdate;
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
}
