package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.InitJoinResponse;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

public interface Node extends Closeable {

    int getNodeId();

    Cluster getCluster();

    Mono<InitJoinResponse> join(Integer joinPort, boolean retry);

}