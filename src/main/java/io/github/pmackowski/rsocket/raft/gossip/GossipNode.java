package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.client.protobuf.JoinRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.JoinResponse;
import io.github.pmackowski.rsocket.raft.client.protobuf.LeaveRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.LeaveResponse;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.netty.channel.socket.DatagramPacket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static io.github.pmackowski.rsocket.raft.gossip.PingUtils.toPing;

public class GossipNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipNode.class);

    private Duration indirectStart = Duration.ofMillis(400);
    private Duration protocolPeriod = Duration.ofSeconds(2);
    private int subgroupSize = 2;

    private AtomicLong protocolPeriodCounter = new AtomicLong(0);
    private GossipOnPingDelay onPingDelay;
    private BooleanSupplier REPEAT_PROBE = () -> true;

    int nodeId;

    private Peers peers;
    private Gossips gossips;
    private Disposable disposable;

    public GossipNode(InnerNode node) {

    }

    public GossipNode(int nodeId) {
        this(nodeId, GossipOnPingDelay.NO_DELAY);
    }

    public GossipNode(int nodeId, GossipOnPingDelay onPingDelay) {
        this.nodeId = nodeId;
        this.onPingDelay = onPingDelay;
        this.gossips = new Gossips(nodeId);
        this.peers = new Peers();
    }

    public void dispose() {
        disposable.dispose();
    }

    public void start() {
        disposable = probeNode()
                .doOnNext(acks -> this.gossips.addGossips(acks))
                .doOnError(throwable -> {
//                    this.gossips.addGossip(destinationNodeId, Gossip.Suspicion.SUSPECT);
                })
                .doFinally(signalType -> {
        //                Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        //                this.gossips.addGossip(nodeId, suspicion);
                    LOGGER.info("[Node {}][ping] Probing {} finished.", this.nodeId, 2313123);
                })
                .repeat(REPEAT_PROBE)
                .subscribe();
    }

    private Flux<Collection<? super Ack>> probeNode() {
        GossipProbe gossipProbe = new GossipProbe(this);
        return Flux.defer(() -> {
            protocolPeriodCounter.incrementAndGet();
            int destinationNodeId = peers.nextRandomPeerId();
            List<Integer> proxyNodeIds = peers.selectCompanions(destinationNodeId, subgroupSize);
            return gossipProbe.probeNode(destinationNodeId, proxyNodeIds, gossips.share(), Mono.delay(indirectStart), Mono.delay(protocolPeriod));
        });
    }

    // The new member does a full state sync with the existing member over TCP and begins gossiping its existence to the cluster.

    // Join is used to take an existing Memberlist and attempt to join a cluster
    // by contacting all the given hosts and performing a state sync. Initially,
    // the Memberlist only contains our own state, so doing this will cause
    // remote nodes to become aware of the existence of this node, effectively
    // joining the cluster.
    //
    // This returns the number of hosts successfully contacted and an error if
    // none could be reached. If an error is returned, the node did not successfully
    // join the cluster.
    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        // other node asked me to join
//        cluster.addMember(joinRequest1.getPort());
//        senders.addServer(joinRequest1.getPort());
        return Mono.just(joinRequest)
                   // TODO check credentials
                   .doOnNext(joinRequest1 -> peers.add(joinRequest1.getPort()))
                   .thenReturn(JoinResponse.newBuilder().setStatus(true).build());
    }

    // Leave will broadcast a leave message but will not shutdown the background
    // listeners, meaning the node will continue participating in gossip and state
    // updates.
    //
    // This will block until the leave message is successfully broadcasted to
    // a member of the cluster, if any exist or until a specified timeout
    // is reached.
    //
    // This method is safe to call multiple times, but must not be called
    // after the cluster is already shut down.
    public Mono<LeaveResponse> onLeaveRequest(LeaveRequest leaveRequest) {
//        peers.remove(leaveRequest.get);
        return Mono.just(leaveRequest)
                   // should propagate to cluster that I am leaving
                   .thenReturn(LeaveResponse.newBuilder().setStatus(true).build());
    }

    public Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
        return udpInbound.receiveObject()
                .cast(DatagramPacket.class)
                .flatMap(datagramPacket -> {
                    Ping ping = toPing(datagramPacket);
                    log(ping);
                    List<Gossip> sharedGossips = gossips.mergeAndShare(ping.getGossipsList());
                    Ack ack = Ack.newBuilder().setNodeId(nodeId).addAllGossips(sharedGossips).build();

                    Publisher<?> publisher;
                    if (ping.getDirect()) {
                        publisher = onPingDelay.apply(ping.getRequestorNodeId(), protocolPeriodCounter.get());
                    } else {
                        GossipTransport gossipTransport = new GossipTransport();
                        Ping newPing = PingUtils.direct(ping, sharedGossips);
                        publisher = gossipTransport.ping(newPing); // TODO handle ack and timeout
                    }
                    return Flux.from(publisher).then(udpOutbound.sendObject(AckUtils.toDatagram(ack, datagramPacket.sender())).then());
                })
                .then();
    }

    private void log(Ping ping) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
                LOGGER.info("[Node {}][onPing] I am being probed by {}", ping.getDestinationNodeId(), ping.getRequestorNodeId());
            } else {
                LOGGER.info("[Node {}][onPing] I am being probed by {} on behalf of {}", ping.getDestinationNodeId(), ping.getRequestorNodeId(), ping.getInitiatorNodeId());
            }
        }
    }
}
