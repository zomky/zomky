package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.*;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingReqRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingRequest;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.raft.RaftGroups;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import reactor.core.publisher.Mono;

public interface InnerNode extends Node {

    Senders getSenders();

    RaftGroups getRaftGroups();

    void senderAvailable(Sender sender);

    void senderUnavailable(Sender sender);

    void onSenderAvailable(SenderAvailableListener senderAvailableListener);

    void onSenderUnavailable(SenderUnavailableListener senderUnavailableListener);

    Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest);

    Mono<InitJoinResponse> onInitJoinRequest(InitJoinRequest initJoinRequest);

    Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest);

    Mono<Void> onPingRequest(PingRequest pingRequest);

    Mono<Void> onPingReqRequest(PingReqRequest pingReqRequest);
}
