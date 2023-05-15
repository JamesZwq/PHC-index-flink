package main.PHCIndex.CoreDecomposition_base;

public class CDMessage<K> {
    private final K source;
    private final int core;

    public CDMessage(K neighbor, int core) {
        this.source = neighbor;
        this.core = core;
    }

    public K getSource() {
        return source;
    }

    public int getCore() {
        return core;
    }

    @Override
    public String toString() {
        return "{" + source + ", " + core + '}';
    }
}
