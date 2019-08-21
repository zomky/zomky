package io.github.zomky;

import io.github.zomky.client.protobuf.InfoRequest;
import io.github.zomky.client.protobuf.InfoResponse;
import io.github.zomky.gossip.GossipProtocol;
import io.github.zomky.listener.SenderAvailableListener;
import io.github.zomky.listener.SenderUnavailableListener;
import io.github.zomky.raft.RaftProtocol;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.Senders;
import reactor.core.publisher.Mono;

public interface InnerNode extends Node {

    Senders getSenders();

    GossipProtocol getGossipProtocol();

    RaftProtocol getRaftProtocol();

    void senderAvailable(Sender sender);

    void senderUnavailable(Sender sender);

    void onSenderAvailable(SenderAvailableListener senderAvailableListener);

    void onSenderUnavailable(SenderUnavailableListener senderUnavailableListener);

    Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest);

}
