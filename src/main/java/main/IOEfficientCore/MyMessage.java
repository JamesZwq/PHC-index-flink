package main.IOEfficientCore;

public class MyMessage<K> {
    private K vertex;
    private Integer core;
    private int cnt;

    public MyMessage(K vertex, int core, int cnt) {
        this.vertex = vertex;
        this.core = core;
        this.cnt = cnt;
    }

    public K getVertex() {
        return vertex;
    }

    public Integer getCore() {
        return core;
    }

    public int getCnt() {
        return cnt;
    }

}
