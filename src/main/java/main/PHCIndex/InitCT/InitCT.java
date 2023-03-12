package main.PHCIndex.InitCT;


import main.PHCIndex.NeighborsValue;
import main.PHCIndex.VertexValue;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public final class InitCT<K> {
    public static final class InitCTMessenger<K>
            extends ScatterFunction<K, VertexValue<K>, InitCTMessage<K>, Integer> {
        private final int timeEnd;

        public InitCTMessenger(int timeStart) {
            this.timeEnd = timeStart;
        }

        @Override
        public void preSuperstep() throws Exception {
            super.preSuperstep();
        }

        @Override
        public void sendMessages(Vertex<K, VertexValue<K>> vertex) throws Exception {
            HashSet<K> visited = new HashSet<>();
            if(!vertex.getValue().isCalculated()) {
                for (Edge<K, Integer> e : getEdges()) {
                    if (e.getValue() > timeEnd) continue;
//                    if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                    if (visited.contains(e.getTarget())) continue;
                    NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                    if(!neighborsValue.decreaseCTN()){
                        sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
                        visited.add(e.getTarget());
                    }
                }
            } else {
                for (Edge<K, Integer> e : getEdges()) {
                    if (e.getValue() > timeEnd) continue;
//                    if (!vertex.getValue().getNeighbors().containsKey(e.getTarget())) continue;
                    if (visited.contains(e.getTarget())) continue;
                    NeighborsValue neighborsValue = vertex.getValue().getNeighbors().get(e.getTarget());
                    if (vertex.getValue().getCore() < neighborsValue.getCore() && neighborsValue.getCore() <= vertex.getValue().getOldCore()) {
                        sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), true));
                        visited.add(e.getTarget());
                    } else if (vertex.getValue().getCore() != vertex.getValue().getOldCore()) {
                        sendMessageTo(e.getTarget(), new InitCTMessage<>(vertex.getId(), vertex.getValue().getCore(), false));
                        visited.add(e.getTarget());
                    }
                }
            }
        }
    }

    public static final class InitCTUpdater<K> extends GatherFunction<K, VertexValue<K>, InitCTMessage<K>> {

        private final int timeEnd;

        public InitCTUpdater(int timeEnd) {
            this.timeEnd = timeEnd;
        }

        @Override
        public void preSuperstep() throws Exception {
            super.preSuperstep();
        }

        @Override
        public void updateVertex(Vertex<K, VertexValue<K>> vertex, MessageIterator<InitCTMessage<K>> inMessages) throws Exception {
            VertexValue<K> v = new VertexValue<>(vertex.getValue());
            boolean noUpdate = true;
            for (InitCTMessage<K> msg : inMessages) {
                v.getNeighbors().get(msg.getSource()).setCore(msg.getCore());
                if(v.isCalculated()){
                    v.getNeighbors().get(msg.getSource()).setCTNtoZero();
                    if(msg.shouldUpdate && v.getCTNSize() < v.getCore()){
                        noUpdate = false;
                    }
                } else {
                    noUpdate = false;
                }
            }
            if (noUpdate) {
                setNewVertexValue(v);
                return;
            }

            v.setCalculatedCoreCN();
            int oldCore = v.getCore();
            v.setOldCore(oldCore);

            List<Integer> cnt = new ArrayList<>();
            for (int i = 0; i <= oldCore; ++i) {
                cnt.add(0);
            }

            HashMap<K, NeighborsValue> neighbors = v.getNeighbors();
            for (K nei : neighbors.keySet()) {
                if(neighbors.get(nei).getCoreTimeNb().stream().noneMatch(x -> x < timeEnd)) continue;
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

            v.resetCoreTimeNeighbors();
            for (K nei : neighbors.keySet()) {
                if(neighbors.get(nei).getCoreTimeNb().stream().noneMatch(x -> x <= timeEnd)) continue;
                int coreNei = neighbors.get(nei).getCore();
                if (coreNei < v.getCore()) continue;
                v.getNeighbors().get(nei).increaseCTN();
            }

            for(int tmp_k = oldCore; tmp_k >= v.getCore(); --tmp_k){
                v.addCoreTime(tmp_k, timeEnd);
            }

            if(!v.equals(vertex.getValue())) setNewVertexValue(v);
        }
    }
}