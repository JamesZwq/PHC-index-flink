package main.PHCIndex.CoreDecomposition;

public class CDMessage<K> {
    private final K source;
    private final int core;
    private final int oldCore;

    private final boolean decreaseCnt;

    public CDMessage(K neighbor, int core, int oldCore, boolean decreaseCnt) {
        this.source = neighbor;
        this.core = core;
        this.oldCore = oldCore;
        this.decreaseCnt = decreaseCnt;
    }

    public K getSource() {
        return source;
    }

    public int getCore() {
        return core;
    }

    public int getOldCore() {
        return oldCore;
    }

    public boolean isDecreaseCnt() {
        return decreaseCnt;
    }
}
