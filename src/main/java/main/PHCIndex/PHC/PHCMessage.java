package main.PHCIndex.PHC;

import java.util.ArrayList;

public class PHCMessage<K> {
    private final K vertexId;
    private final CoreTimes coreTime;

    public PHCMessage(K vertexId, CoreTimes coreTime) {
        this.vertexId = vertexId;
        this.coreTime = coreTime;
    }

    public K getVertexId() {
        return vertexId;
    }

    public CoreTimes getCoreTime() {
        return coreTime;
    }
}
