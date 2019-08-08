package io.github.pmackowski.rsocket.raft.gossip.listener;

public interface NodeLeftForcefullyListener {

    void handle(int nodeId);

}
