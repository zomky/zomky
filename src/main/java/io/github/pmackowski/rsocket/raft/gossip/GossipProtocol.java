package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.*;
import io.github.pmackowski.rsocket.raft.gossip.transport.GossipTcpTransport;
import io.netty.channel.socket.DatagramPacket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.pmackowski.rsocket.raft.gossip.AckUtils.nack;
import static io.github.pmackowski.rsocket.raft.gossip.PingUtils.toPing;

public class GossipProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

    private int nodeId;
    private Cluster cluster;
    private Peers peers;
    private Gossips gossips;
    private RandomGossipProbe randomGossipProbe;
    private GossipTransport gossipTransport;
    private GossipOnPingDelay onPingDelay;
    private Disposable probeNodesDisposable;
    private Disposable peerChangesDisposable;

    public void dispose() {
        probeNodesDisposable.dispose();
        peerChangesDisposable.dispose();
    }

    public void start() {
        peerChangesDisposable = gossips.peerChanges()
                .subscribe(suspicion -> {
                    if (suspicion.getSuspicion() == Gossip.Suspicion.ALIVE) {
                        peers.add(suspicion.getNodeId());
                        cluster.addMember(suspicion.getNodeId());
                    }
                }, throwable -> {
                    LOGGER.info("[Node {}] unrecoverable error", nodeId);
                });
        probeNodesDisposable = probeNodes().subscribe();
    }

    // visible for testing
    Flux<ProbeResult> probeNodes() {
        AtomicBoolean wasFirstSubscription = new AtomicBoolean(false);

        return Mono.defer(() -> randomGossipProbe.randomProbe())
            .doOnNext(probeResult -> randomGossipProbe.probeCompleted(probeResult))
            .delaySubscription(Mono.defer(() -> {
                if (wasFirstSubscription.compareAndSet(false, true)) {
                    return Mono.empty();
                }
                return Mono.delay(randomGossipProbe.probeInterval());
            }))
            .repeat()
            .doOnError(throwable -> LOGGER.error("[Node {}] Probe nodes job has been stopped!", nodeId, throwable));
    }

    public Mono<InitJoinResponse> join(InitJoinRequest initJoinRequest) {
        // trying to join other node
        JoinRequest joinRequest = JoinRequest.newBuilder()
                .setPort(initJoinRequest.getPort())
                .setRequesterPort(initJoinRequest.getRequesterPort())
                .addAllGossips(gossips.allGossips())
                .build();
        return Mono.defer(() -> GossipTcpTransport.join(joinRequest))
            .doOnNext(joinResponse -> gossips.addGossips(joinResponse.getGossipsList()))
            .retryWhen(Retry
                    .onlyIf(objectRetryContext -> initJoinRequest.getRetry())
                    .fixedBackoff(Duration.ofSeconds(1))
                    .doOnRetry(context -> LOGGER.warn("[Node {}] Join {} failed. Retrying ...", nodeId, initJoinRequest.getPort()))
            )
            .thenReturn(InitJoinResponse.newBuilder()
                    .setStatus(true)
                    .build()
            );
    }

    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        // other node asked me to join
        return Mono.fromRunnable(() -> gossips.addGossips(joinRequest.getGossipsList()))
                   // TODO check credentials
                   .thenReturn(JoinResponse.newBuilder()
                           .setStatus(true)
                           .addAllGossips(gossips.allGossips())
                           .build()
                   );
    }

    public Mono<InitLeaveResponse> leave(InitLeaveRequest initLeaveRequest) {
        // I am leaving, lets inform other nodes
        LOGGER.info("[Node {}] Leaving cluster ...");
        return Mono.fromRunnable(() -> gossips.addGossip(Gossip.newBuilder().setSuspicion(Gossip.Suspicion.DEAD).build()))
                // TODO check credentials
                .then(Mono.delay(Duration.ofSeconds(5)).doOnSuccess(l -> System.exit(0)))
                .thenReturn(InitLeaveResponse.newBuilder()
                        .build()
                );
    }

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
                    try {
                        Ping ping = toPing(datagramPacket);
                        logOnPing(ping);
                        checkPing(ping);
                        Ack ack = gossips.onPing(nodeId, peers.count(), ping);
                        DatagramPacket ackDatagram = AckUtils.toDatagram(ack, datagramPacket.sender());

                        Flux<?> publisher;
                        if (ping.getDirect()) {
                            publisher = onPingDelay
                                    .apply(ping.getRequestorNodeId(), ping.getCounter())
                                    .thenReturn(ackDatagram)
                                    .onErrorResume(throwable -> {
                                        LOGGER.warn("[Node {}][onPing] Internal error caused by 'onPingDelay'", ping.getRequestorNodeId());
                                        return Mono.empty();
                                    })
                                    .flux();
                        } else {
                            Ping pingOnBehalf = PingUtils.direct(ping, ack.getGossipsList());
                            Mono<Ack> nackMono = Mono.just(nack(ack)).delayElement(Duration.ofMillis(ping.getNackTimeout()));
                            DatagramPacket nackDatagram = AckUtils.toDatagram(nack(ack), datagramPacket.sender());

                            publisher = Flux.merge(gossipTransport.ping(pingOnBehalf), nackMono)
                                    .takeUntil(indirectAckOrNack -> !indirectAckOrNack.getNack())
                                    .doOnNext(indirectAckOrNack -> {
                                        if (indirectAckOrNack.getNack()) {
                                            LOGGER.warn("[Node {}][onPing] Probe to {} on behalf of {} taking too long time. Sending back NACK but still waiting for ACK.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
                                        } else {
                                            LOGGER.info("[Node {}][onPing] Probe to {} on behalf of {} successful.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
                                            gossips.addAck(indirectAckOrNack);
                                        }
                                    })
                                    .doOnError(throwable -> {
                                        LOGGER.warn("[Node {}][onPing] Probe to {} on behalf of {} failed. Reason {}.", pingOnBehalf.getRequestorNodeId(), pingOnBehalf.getDestinationNodeId(), pingOnBehalf.getInitiatorNodeId(), throwable.getMessage());
                                    })
                                    .map(indirectAckOrNack -> indirectAckOrNack.getNack() ?  nackDatagram : ackDatagram)
                                    // if proxy is available and cannot reach destination then nack is returned
                                    .onErrorReturn(nackDatagram)
                                    .distinct();
                        }
                        return publisher.flatMap(datagram -> udpOutbound.sendObject(datagram)
                                .then()
                                .onErrorResume(throwable -> {
                                    if (ping.getDirect()) {
                                        LOGGER.warn("[Node {}][onPing] Cannot send ack back to {}!", ping.getDestinationNodeId(), ping.getRequestorNodeId());
                                    } else {
                                        LOGGER.warn("[Node {}][onPing] Cannot send ack back to {}!", ping.getRequestorNodeId(), ping.getInitiatorNodeId());
                                    }
                                    return Mono.empty();
                                })
                                .then()
                        );
                    } catch (Exception e) {
                        LOGGER.warn("[Node {}][onPing] error while processing datagram!", nodeId, e);
                        return Mono.empty();
                    }
                });
    }

    private void checkPing(Ping ping) {
        if (ping.getDirect()) {
            checkDirectPing(ping);
        } else {
            checkIndirectPing(ping);
        }
    }

    private void checkDirectPing(Ping ping) {
        if (ping.getDestinationNodeId() != nodeId) {
            throw new GossipException(String.format("This node is not a destination node! [%s,%s]", ping.getDestinationNodeId(), nodeId));
        }
    }

    private void checkIndirectPing(Ping ping) {
        if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
            throw new GossipException(String.format("Initiator and requestor node must not be the same! [%s,%s]", ping.getInitiatorNodeId(), ping.getRequestorNodeId()));
        }
        if (ping.getRequestorNodeId() != nodeId) {
            throw new GossipException(String.format("This node is not a proxy node! [%s,%s]", ping.getRequestorNodeId(), nodeId));
        }
    }

    private static void logOnPing(Ping ping) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
                LOGGER.info("[Node {}][onPing] I am being probed by {} ...", ping.getDestinationNodeId(), ping.getRequestorNodeId());
            } else {
                LOGGER.info("[Node {}][onPing] I am being probed by {} on behalf of {} ...", ping.getDestinationNodeId(), ping.getRequestorNodeId(), ping.getInitiatorNodeId());
            }
        } else {
            LOGGER.info("[Node {}][onPing] Probing {} on behalf of {} ...", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
        }
    }

    private GossipProtocol() {}

    public static GossipProtocol.Builder builder() {
        return new GossipProtocol.Builder();
    }

    public static class Builder {

        private int nodeId;
        private Duration baseProbeInterval;
        private Duration baseProbeTimeout;
        private int subgroupSize;
        private int maxGossips;
        private float lambdaGossipSharedMultiplier;
        private float indirectDelayRatio;
        private float nackRatio;
        private int maxLocalHealthMultiplier;

        private Builder() {
        }

        public GossipProtocol.Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public GossipProtocol.Builder baseProbeTimeout(Duration baseProbeTimeout) {
            this.baseProbeTimeout = baseProbeTimeout;
            return this;
        }

        public GossipProtocol.Builder baseProbeInterval(Duration baseProbeInterval) {
            this.baseProbeInterval = baseProbeInterval;
            return this;
        }

        public GossipProtocol.Builder subgroupSize(int subgroupSize) {
            this.subgroupSize = subgroupSize;
            return this;
        }

        public GossipProtocol.Builder maxGossips(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public GossipProtocol.Builder lambdaGossipSharedMultiplier(float lambdaGossipSharedMultiplier) {
            this.lambdaGossipSharedMultiplier = lambdaGossipSharedMultiplier;
            return this;
        }

        public GossipProtocol.Builder indirectDelayRatio(float indirectDelayRatio) {
            this.indirectDelayRatio = indirectDelayRatio;
            return this;
        }

        public GossipProtocol.Builder nackRatio(float nackRatio) {
            this.nackRatio = nackRatio;
            return this;
        }

        public GossipProtocol.Builder maxLocalHealthMultiplier(int maxLocalHealthMultiplier) {
            this.maxLocalHealthMultiplier = maxLocalHealthMultiplier;
            return this;
        }

        public GossipProtocol build() {
            GossipProtocol gossipProtocol = new GossipProtocol();
            gossipProtocol.nodeId = nodeId;
            gossipProtocol.cluster = new Cluster(nodeId);
            gossipProtocol.gossipTransport = new GossipTransport();
            gossipProtocol.peers = new Peers(nodeId);
            gossipProtocol.gossips = Gossips.builder()
                    .nodeId(nodeId)
                    .addAliveGossipAboutItself()
                    .maxGossips(maxGossips)
                    .maxLocalHealthMultiplier(maxLocalHealthMultiplier)
                    .gossipDisseminationMultiplier(lambdaGossipSharedMultiplier)
                    .build();
            gossipProtocol.onPingDelay = GossipOnPingDelay.NO_DELAY;

            gossipProtocol.randomGossipProbe = RandomGossipProbe.builder()
                    .nodeId(nodeId)
                    .peers(gossipProtocol.peers)
                    .gossips(gossipProtocol.gossips)
                    .gossipProbe(new GossipProbe(nodeId, gossipProtocol.gossipTransport))
                    .baseProbeInterval(baseProbeInterval)
                    .baseProbeTimeout(baseProbeTimeout)
                    .subgroupSize(subgroupSize)
                    .indirectDelayRatio(indirectDelayRatio)
                    .nackRatio(nackRatio)
                    .build();

            return gossipProtocol;
        }

    }

    // for testing
    GossipProtocol(int nodeId, Peers peers, Gossips gossips, RandomGossipProbe randomGossipProbe, GossipTransport gossipTransport, GossipOnPingDelay onPingDelay) {
        this.nodeId = nodeId;
        this.peers = peers;
        this.gossips = gossips;
        this.randomGossipProbe = randomGossipProbe;
        this.gossipTransport = gossipTransport;
        this.onPingDelay = onPingDelay;
    }

}
