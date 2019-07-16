package io.github.pmackowski.rsocket.raft;

import io.rsocket.Closeable;

public interface Node extends Closeable {

    RaftGroups getRaftGroups();

    void addGroup(RaftGroup raftGroup);

    RaftConfiguration getRaftConfiguration();

    int getNodeId();

}