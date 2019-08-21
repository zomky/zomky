package io.github.zomky;

import io.github.zomky.client.protobuf.InfoRequest;
import io.github.zomky.client.protobuf.InfoResponse;
import io.github.zomky.gossip.Cluster;
import io.github.zomky.gossip.GossipProtocol;
import io.github.zomky.gossip.listener.NodeJoinedListener;
import io.github.zomky.gossip.listener.NodeLeftGracefullyListener;
import io.github.zomky.gossip.protobuf.InitJoinRequest;
import io.github.zomky.gossip.protobuf.InitJoinResponse;
import io.github.zomky.listener.SenderAvailableListener;
import io.github.zomky.listener.SenderUnavailableListener;
import io.github.zomky.raft.RaftProtocol;
import io.github.zomky.transport.Receiver;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.Senders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Duration;
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

    DefaultNode(NodeStorage nodeStorage, String nodeName, int nodeId, Cluster cluster, Duration baseProbeTimeout) {
        this.nodeStorage = nodeStorage;
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.cluster = cluster; // at the beginning must always contain only this node

        this.receiver = new Receiver(this);
        this.gossipProtocol =  GossipProtocol.builder()
                .nodeId(nodeId)
                .baseProbeTimeout(baseProbeTimeout)
                .baseProbeInterval(Duration.ofMillis(2000))
                .subgroupSize(2)
                .maxGossips(10)
                .lambdaGossipSharedMultiplier(1.2f)
                .indirectDelayRatio(0.3f)
                .nackRatio(0.6f)
                .maxLocalHealthMultiplier(8)
                .build();

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
    public Mono<InitJoinResponse> join(Integer joinPort, boolean retry) {
        return gossipProtocol.join(InitJoinRequest.newBuilder()
                .setRequesterPort(nodeId)
                .setPort(joinPort)
                .setRetry(retry)
                .build());
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
