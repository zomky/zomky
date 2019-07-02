package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

public class PassiveRole implements RaftServerRole {

    @Override
    public NodeState nodeState() {
        return NodeState.PASSIVE;
    }

    @Override
    public void onInit(DefaultRaftServer raftServer, RaftStorage raftStorage) {

    }

    @Override
    public void onExit(DefaultRaftServer raftServer, RaftStorage raftStorage) {

    }
}
