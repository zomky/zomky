package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.*;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.raft.RaftProtocol;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;

public interface InnerNode extends Node {

    Senders getSenders();

    RaftProtocol getRaftProtocol();

    void senderAvailable(Sender sender);

    void senderUnavailable(Sender sender);

    void onSenderAvailable(SenderAvailableListener senderAvailableListener);

    void onSenderUnavailable(SenderUnavailableListener senderUnavailableListener);

    Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest);

    Mono<InitJoinResponse> onInitJoinRequest(InitJoinRequest initJoinRequest);

    Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest);

    Mono<InitLeaveResponse> onInitLeaveRequest(InitLeaveRequest initLeaveRequest);

    Mono<LeaveResponse> onLeaveRequest(LeaveRequest leaveRequest);

    Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound);
}
