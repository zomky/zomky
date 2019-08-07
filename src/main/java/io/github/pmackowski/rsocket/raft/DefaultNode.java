package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.gossip.GossipProtocol;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.*;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.raft.RaftProtocol;
import io.github.pmackowski.rsocket.raft.transport.Receiver;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.Set;

class DefaultNode implements InnerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    private NodeStorage nodeStorage;
    private String nodeName;
    private int nodeId;
    private Cluster cluster;

    private Receiver receiver;
    private Senders senders;
    private GossipProtocol gossipProtocol;
    private RaftProtocol raftProtocol;

    private Set<SenderAvailableListener> senderAvailableListeners = new HashSet<>();
    private Set<SenderUnavailableListener> senderUnavailableListeners = new HashSet<>();

    DefaultNode(NodeStorage nodeStorage, String nodeName, int nodeId, Cluster cluster) {
        this.nodeStorage = nodeStorage;
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.cluster = cluster; // at the beginning must always contain only this node

        this.receiver = new Receiver(this);
        this.gossipProtocol = new GossipProtocol(this);
        this.senders = new Senders(this);
        this.raftProtocol = new RaftProtocol(this);

        LOGGER.info("[Node {}] has been initialized", nodeId);
    }

    public void startReceiver() {
        receiver.start();
    }

    public void start() {
        gossipProtocol.start();
        senders.start();
        raftProtocol.start();
    }

    @Override
    public Senders getSenders() {
        return senders;
    }

    @Override
    public RaftProtocol getRaftProtocol() {
        return raftProtocol;
    }

    @Override
    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public int getNodeId() {
        return nodeId;
    }

    @Override
    public boolean isDisposed() {
        return false;
    }

    @Override
    public Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest) {
//        return Mono.just(InfoResponse.newBuilder().addAllMembers(currentConfiguration.getMembers()).build());
        return Mono.empty();
    }

    public Mono<Void> join(Integer joinPort, boolean retry) {
        return gossipProtocol.join(InitJoinRequest.newBuilder()
                .setRequesterPort(nodeId)
                .setPort(joinPort)
                .setRetry(retry)
                .build()
        ).then();
    }

    @Override
    public Mono<InitJoinResponse> onInitJoinRequest(InitJoinRequest initJoinRequest) {
        return gossipProtocol.join(initJoinRequest);
    }

    @Override
    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        return gossipProtocol.onJoinRequest(joinRequest);
    }

    @Override
    public Mono<InitLeaveResponse> onInitLeaveRequest(InitLeaveRequest initLeaveRequest) {
        return gossipProtocol.onInitLeaveRequest(initLeaveRequest);
    }

    @Override
    public Mono<LeaveResponse> onLeaveRequest(LeaveRequest leaveRequest) {
        return gossipProtocol.onLeaveRequest(leaveRequest);
    }

    @Override
    public Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
        return gossipProtocol.onPing(udpInbound, udpOutbound);
    }

    @Override
    public void onSenderAvailable(SenderAvailableListener senderAvailableListener) {
        senderAvailableListeners.add(senderAvailableListener);
    }

    @Override
    public void onSenderUnavailable(SenderUnavailableListener senderUnavailableListener) {
        senderUnavailableListeners.add(senderUnavailableListener);
    }

    @Override
    public void senderAvailable(Sender sender) {
        senderAvailableListeners.forEach(senderAvailableListener -> senderAvailableListener.handle(sender));
    }

    @Override
    public void senderUnavailable(Sender sender) {
        senderUnavailableListeners.forEach(senderUnavailableListener -> senderUnavailableListener.handle(sender));
    }

    @Override
    public void dispose() {
        LOGGER.info("[Node {}] Stopping ...", nodeId);
        raftProtocol.dispose();
        gossipProtocol.dispose();

        receiver.stop();
        senders.stop();
    }

    @Override
    public Mono<Void> onClose() {
        return Mono.error(new NotImplementedException());
    }

}
