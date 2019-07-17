package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.raft.RaftGroups;
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

    private int nodeId;
    private Cluster cluster;

    private Receiver receiver;
    private Senders senders;
    private RaftGroups raftGroups;
    private Set<SenderAvailableListener> senderAvailableListeners = new HashSet<>();
    private Set<SenderUnavailableListener> senderUnavailableListeners = new HashSet<>();

    DefaultNode(int nodeId, Cluster cluster) {
        this.nodeId = nodeId;
        this.cluster = cluster;

        this.receiver = new Receiver(this);
        this.senders = new Senders(this);
        this.raftGroups = new RaftGroups(this);

        LOGGER.info("[Node {}] has been initialized", nodeId);
    }

    public void start() {
        receiver.start();
        senders.start();
        raftGroups.start();
    }

    @Override
    public Senders getSenders() {
        return senders;
    }

    @Override
    public RaftGroups getRaftGroups() {
        return raftGroups;
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
        raftGroups.dispose();

        receiver.stop();
        senders.stop();
    }

    @Override
    public Mono<Void> onClose() {
        return Mono.error(new NotImplementedException());
    }

}
