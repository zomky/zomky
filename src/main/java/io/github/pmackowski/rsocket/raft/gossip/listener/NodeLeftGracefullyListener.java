package io.github.pmackowski.rsocket.raft.gossip.listener;

public interface NodeLeftGracefullyListener {

    void handle(int nodeId);

}
