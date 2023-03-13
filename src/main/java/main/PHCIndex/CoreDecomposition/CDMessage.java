package main.PHCIndex.CoreDecomposition;

public class CDMessage<K> {
    private K source;
    private int core;
    private int cnt;

    private boolean decreaseCnt;

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

    public void setSource(K source) {
        this.source = source;
    }

    public void setCore(int core) {
        this.core = core;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
