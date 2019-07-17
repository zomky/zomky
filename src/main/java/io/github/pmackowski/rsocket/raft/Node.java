package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.raft.RaftGroups;
import io.rsocket.Closeable;

public interface Node extends Closeable {

    int getNodeId();

    RaftGroups getRaftGroups();

    Cluster getCluster();
}