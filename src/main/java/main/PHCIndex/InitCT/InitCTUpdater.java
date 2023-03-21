package main.PHCIndex.InitCT;


import main.PHCIndex.PHCVertex.NeighborsValue;
import main.PHCIndex.PHCVertex.VertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * @param <K> The type of the vertex key.
 */
public final class InitCTUpdater<K> extends GatherFunction<K, VertexValue<K>, InitCTMessage<K>> {

    private final int timeEnd;

    public InitCTUpdater(int timeEnd) {
        this.timeEnd = timeEnd;
    }

    @Override
    public void preSuperstep() throws Exception {
        super.preSuperstep();
    }

    @Override
    public void postSuperstep() throws Exception {
        super.postSuperstep();
    }

    @Override
    public void updateVertex(Vertex<K, VertexValue<K>> vertex, MessageIterator<InitCTMessage<K>> inMessages) throws Exception {
        VertexValue<K> v = new VertexValue<>(vertex.getValue());
        boolean noUpdate = true;
        for (InitCTMessage<K> msg : inMessages) {
            v.insertNeighbors(msg.getSource(), msg.getCore(), msg.getMinTime());
            if(v.isCalculated()){
                if(msg.isShouldUpdate() && v.getCTNSize() < v.getCore()){
                    noUpdate = false;
                }
            } else {
                noUpdate = false;
            }
        }

        if (noUpdate || getSuperstepNumber() == 1) {
            setNewVertexValue(v);
            return;
        }
        v.setCalculatedCoreCN();

//        对比core
        int oldCore = v.getCore();
        v.setOldCore(oldCore);

        List<Integer> cnt = new ArrayList<>();
        for (int i = 0; i <= oldCore; ++i) {
            cnt.add(0);
        }

        HashMap<K, NeighborsValue> neighbors = v.getNeighbors();
        for (K nei : neighbors.keySet()) {
            if(neighbors.get(nei).getMinTime() > timeEnd) continue;
            int coreNei = neighbors.get(nei).getCore();
            if (coreNei < oldCore) cnt.set(coreNei, cnt.get(coreNei) + 1);
            else cnt.set(oldCore, cnt.get(oldCore) + 1);
        }
        int cd = 0;
        for (int k = oldCore; k >= 0 ; --k) {
            cd += cnt.get(k);
            if(cd >= k){
                v.setCore(k);
                break;
            }
        }

//        计算CTN
        v.resetCoreTimeNeighbors();
        for (K nei : neighbors.keySet()) {
            if(neighbors.get(nei).getMinTime() > timeEnd) continue;
            int coreNei = neighbors.get(nei).getCore();
            if (coreNei < v.getCore()) continue;
            v.getNeighbors().get(nei).increaseCTN();
        }

        for(int tmp_k = oldCore; tmp_k >= v.getCore(); --tmp_k){
            v.addCoreTime(tmp_k, timeEnd);
        }

        if(!v.equals(vertex.getValue()))
            setNewVertexValue(v);
    }
}