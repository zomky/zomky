package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.gossip.GossipProtocol;
import io.github.pmackowski.rsocket.raft.gossip.listener.NodeJoinedListener;
import io.github.pmackowski.rsocket.raft.gossip.listener.NodeLeftGracefullyListener;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.InitJoinRequest;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.raft.RaftProtocol;
import io.github.pmackowski.rsocket.raft.transport.Receiver;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
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

    private Set<NodeJoinedListener> nodeJoinedListeners = new HashSet<>();
    private Set<NodeLeftGracefullyListener> nodeLeftGracefullyListeners = new HashSet<>();

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
    public GossipProtocol getGossipProtocol() {
        return gossipProtocol;
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

    @Override
    public Mono<Void> join(Integer joinPort, boolean retry) {
        return gossipProtocol.join(InitJoinRequest.newBuilder()
                .setRequesterPort(nodeId)
                .setPort(joinPort)
                .setRetry(retry)
                .build()
        ).then();
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
    public void onNodeJoined(NodeJoinedListener nodeJoinedListener) {
        nodeJoinedListeners.add(nodeJoinedListener);
    }

    @Override
    public void onNodeLeftGracefully(NodeLeftGracefullyListener nodeLeftGracefullyListener) {
        nodeLeftGracefullyListeners.add(nodeLeftGracefullyListener);
    }

    @Override
    public void nodeJoined(int nodeId) {
        // TODO use one of reactor processors instead
        nodeJoinedListeners.forEach(nodeJoinedListener -> nodeJoinedListener.handle(nodeId));
    }

    @Override
    public void nodeLeftGracefully(int nodeId) {
        // TODO use one of reactor processors instead
        nodeLeftGracefullyListeners.forEach(nodeLeftGracefullyListener -> nodeLeftGracefullyListener.handle(nodeId));
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
