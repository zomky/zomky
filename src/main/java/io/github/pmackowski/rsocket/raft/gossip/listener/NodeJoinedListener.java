package io.github.pmackowski.rsocket.raft.gossip.listener;

public interface NodeJoinedListener {

    void handle(int nodeId);

}
