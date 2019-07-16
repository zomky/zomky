package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.transport.Receiver;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.Set;

class DefaultNode implements InnerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    int nodeId;
    private RaftConfiguration raftConfiguration;
    private RaftStorageConfiguration raftStorageConfiguration;
    private Cluster cluster;

    private Receiver receiver;
    private Senders senders;
    private Set<SenderAvailableListener> senderAvailableListeners = new HashSet<>();
    private Set<SenderUnavailableListener> senderUnavailableListeners = new HashSet<>();

    private RaftGroups raftGroups;

    DefaultNode(int nodeId,
                RaftConfiguration raftConfiguration,
                RaftStorageConfiguration raftStorageConfiguration,
                Cluster cluster) {
        this.nodeId = nodeId;
        this.raftConfiguration = raftConfiguration;
        this.raftStorageConfiguration = raftStorageConfiguration;
        this.cluster = cluster;

        this.raftGroups = new RaftGroups(this);
        this.receiver = new Receiver(this);
        this.senders = new Senders(this, this.nodeId);

        LOGGER.info("[Node {}] has been initialized", nodeId);
    }

    public void start() {
        receiver.start();
        senders.start();

        raftGroups.start(this, raftStorage);
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
    public RaftConfiguration getRaftConfiguration() {
        return raftConfiguration;
    }

    @Override
    public int getNodeId() {
        return 0;
    }

    @Override
    public void dispose() {
        LOGGER.info("[Node {}] Stopping ...", nodeId);
        raftGroups.dispose(this, raftStorage);

        receiver.stop();
        senders.stop();
    }

    public void startGroups() {
        raftGroups.start(this, raftStorage);
    }

    Mono<Sender> createSender(AddServerRequest addServerRequest) {
        return Mono.fromCallable(() -> Sender.createSender(addServerRequest.getNewServer())).cache().subscribeOn(Schedulers.elastic());
    }

    @Override
    public Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest) {
//        return Mono.just(InfoResponse.newBuilder().addAllMembers(currentConfiguration.getMembers()).build());
        return Mono.empty();
    }

    @Override
    public void addGroup(RaftGroup raftGroup) {
        raftGroups.addGroup(raftGroup);
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
    public Mono<Void> onClose() {
        return Mono.error(new NotImplementedException());
    }

}
