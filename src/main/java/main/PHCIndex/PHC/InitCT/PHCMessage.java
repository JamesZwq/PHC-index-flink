package main.PHCIndex.PHC.InitCT;



public class PHCMessage<K> {
    int core;
    K u;

    boolean shouldUpdate;

    public PHCMessage(K u, int core, boolean shouldUpdate) {
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
