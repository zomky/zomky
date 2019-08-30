package io.github.zomky;

import io.github.zomky.client.protobuf.InfoRequest;
import io.github.zomky.client.protobuf.InfoResponse;
import io.github.zomky.gossip.GossipProtocol;
import io.github.zomky.raft.RaftProtocol;
import reactor.core.publisher.Mono;

public interface InnerNode extends Node {

    GossipProtocol getGossipProtocol();

    RaftProtocol getRaftProtocol();

    Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest);

}
