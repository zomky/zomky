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

    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
//        cluster.addMember(joinRequest1.getPort());
//        senders.addServer(joinRequest1.getPort());
        peers.add(joinRequest.getPort());
        return null;
    }

    public Mono<LeaveResponse> onLeaveRequest(LeaveRequest leaveRequest) {
//        peers.remove(leaveRequest.get);
        return null;
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
