package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.gossip.listener.NodeJoinedListener;
import io.github.pmackowski.rsocket.raft.gossip.listener.NodeLeftGracefullyListener;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.InitJoinResponse;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

public interface Node extends Closeable {

    int getNodeId();

    Cluster getCluster();

    Mono<InitJoinResponse> join(Integer joinPort, boolean retry);

    void onNodeJoined(NodeJoinedListener nodeJoinedListener);

    void onNodeLeftGracefully(NodeLeftGracefullyListener nodeLeftGracefullyListener);

}