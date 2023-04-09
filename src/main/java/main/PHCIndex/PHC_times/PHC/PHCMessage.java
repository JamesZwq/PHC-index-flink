package main.PHCIndex.PHC_times.PHC;

import org.apache.commons.math3.analysis.function.Abs;

import java.util.ArrayList;

public class PHCMessage<K> {
    private final K vertexId;
    private final CoreTimes coreTime;
    private final ArrayList<Boolean> updateAt;

    private final int core;

    public PHCMessage(K vertexId, CoreTimes coreTime, ArrayList<Boolean> updateAt, int core){
        this.vertexId = vertexId;
        this.coreTime = coreTime;
        this.updateAt = updateAt;
        this.core = core;
    }

    public K getVertexId() {
        return vertexId;
    }

    public CoreTimes getCoreTime() {
        return coreTime;
    }

    public ArrayList<Boolean> getUpdateAt() {
        return updateAt;
    }

    public int getCore() {
        return core;
    }
}
