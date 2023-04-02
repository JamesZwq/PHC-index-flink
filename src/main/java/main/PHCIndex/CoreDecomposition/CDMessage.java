package main.PHCIndex.CoreDecomposition;

public class CDMessage<K> {
    private final K source;
    private final int core;
    private final int cnt;

    private final boolean decreaseCnt;

    public CDMessage(K neighbor, int core, int cnt, boolean decreaseCnt) {
        this.source = neighbor;
        this.core = core;
        this.cnt = cnt;
        this.decreaseCnt = decreaseCnt;
    }

    public K getSource() {
        return source;
    }

    public int getCore() {
        return core;
    }

    public int getCnt() {
        return cnt;
    }

    public boolean isDecreaseCnt() {
        return decreaseCnt;
    }
}
