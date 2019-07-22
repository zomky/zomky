package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.*;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.gossip.GossipReceiver;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingReqRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingRequest;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

class DefaultNode implements InnerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    private NodeStorage nodeStorage;
    private String nodeName;
    private int nodeId;
    private Cluster cluster;

    private GossipReceiver gossipReceiver;
    private Receiver receiver;
    private Senders senders;
    private RaftGroups raftGroups;
    private Set<SenderAvailableListener> senderAvailableListeners = new HashSet<>();
    private Set<SenderUnavailableListener> senderUnavailableListeners = new HashSet<>();

    DefaultNode(NodeStorage nodeStorage, String nodeName, int nodeId, Cluster cluster) {
        this.nodeStorage = nodeStorage;
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.cluster = cluster;

        this.gossipReceiver = new GossipReceiver(this);
        this.receiver = new Receiver(this);
        this.senders = new Senders(this);
        this.raftGroups = new RaftGroups(this);

        LOGGER.info("[Node {}] has been initialized", nodeId);
    }

    public void start() {
        gossipReceiver.start();
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
    public Mono<InitJoinResponse> onInitJoinRequest(InitJoinRequest initJoinRequest) {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            senders.addServer(initJoinRequest.getPort()); // TODO
            return senders.getSenderById(initJoinRequest.getPort())
                    .flatMap(sender -> sender.join(JoinRequest.newBuilder().setHost(inetAddress.getHostAddress()).setPort(initJoinRequest.getRequesterPort()).build()))
                    .doOnNext(joinResponse -> {
                        LOGGER.info("[Node {}] onInitJoinRequest {}", nodeId, initJoinRequest);

                        if (joinResponse.getStatus()) {
                            cluster.addMember(initJoinRequest.getPort());
                            senders.addServer(initJoinRequest.getPort());
                        }
                    })
                    .thenReturn(InitJoinResponse.newBuilder().setStatus(true).build());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        return Mono.just(joinRequest)
                   .doOnNext(joinRequest1 -> {
                       LOGGER.info("[Node {}] onJoinRequest {}", nodeId, joinRequest);
                       cluster.addMember(joinRequest1.getPort());
                       senders.addServer(joinRequest1.getPort());
                   })
                   .thenReturn(JoinResponse.newBuilder().setStatus(true).build());
    }

    // gossip
    @Override
    public Mono<Void> onPingRequest(PingRequest pingRequest) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> onPingReqRequest(PingReqRequest pingReqRequest) {
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
