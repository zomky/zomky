package io.github.zomky;

import io.github.zomky.gossip.protobuf.InitJoinResponse;
import io.github.zomky.raft.RaftProtocol;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

public interface Node extends Closeable {

    int getNodeId();

    Cluster getCluster();

    Mono<InitJoinResponse> join(Integer joinPort, boolean retry);

    RaftProtocol getRaftProtocol();
}