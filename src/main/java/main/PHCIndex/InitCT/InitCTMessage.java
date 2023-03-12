package main.PHCIndex.InitCT;



public class InitCTMessage<K> {
    int core;
    K u;

    boolean shouldUpdate;

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
