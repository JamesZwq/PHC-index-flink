package main.IOEfficientCore;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Objects;

public class MyVertex<K> {
    K key;
    Integer core;
    Integer cut;
//    HashMap<K, Tuple2<CoreNumber,CntNumber>> neighbors;
    HashMap<K, Tuple2<Integer,Integer>> neighbors;

    public MyVertex(K key, Integer core, Integer cut, HashMap<K, Tuple2<Integer,Integer>> neighbors) {
        this.key = key;
        this.core = core;
        this.cut = cut;
        this.neighbors = new HashMap<>(neighbors);
    }

    public MyVertex(MyVertex<K> v) {
        this.key = v.key;
        this.core = v.core;
        this.cut = v.cut;
        this.neighbors = new HashMap<>(v.neighbors);
    }

    public K getKey() {
        return key;
    }

    public Integer getCore() {
        return core;
    }

    public Integer getCut() {
        return cut;
    }

    public HashMap<K, Tuple2<Integer,Integer>> getNeighbors() {
        return neighbors;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setCore(Integer core) {
        this.core = core;
    }

    public void setCut(Integer cut) {
        this.cut = cut;
    }

    public void setNeighbors(HashMap<K, Tuple2<Integer,Integer>> neighbors) {
        this.neighbors = neighbors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyVertex<?> myVertex = (MyVertex<?>) o;
//        for (K key : neighbors.keySet()) {
//            if (!myVertex.neighbors.containsKey(key)) {
//                return false;
//            }
//            if (!myVertex.neighbors.get(key).equals(neighbors.get(key))) {
//                return false;
//            }
//        }
        return Objects.equals(key, myVertex.key) &&
                core.equals(myVertex.core) &&
                cut.equals(myVertex.cut) &&
                neighbors.equals(myVertex.neighbors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, core, cut, neighbors);
    }

    @Override
    public String toString() {
        return "MyVertex{" +
                "key=" + key +
                ", core=" + core +
                ", cut=" + cut +
                ", neighbors=" + neighbors +
                '}';
    }
}
