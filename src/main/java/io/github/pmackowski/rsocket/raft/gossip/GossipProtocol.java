package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.InnerNode;
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
import java.util.function.BooleanSupplier;

import static io.github.pmackowski.rsocket.raft.gossip.AckUtils.nack;
import static io.github.pmackowski.rsocket.raft.gossip.PingUtils.toPing;

public class GossipProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

    private Duration indirectStart = Duration.ofMillis(400);
    private Duration protocolPeriod = Duration.ofSeconds(2);
    private int subgroupSize = 2;
    private int maxGossips = 10;

    private GossipOnPingDelay onPingDelay;
    private BooleanSupplier REPEAT_PROBE = () -> true;

    private InnerNode node;
    private Cluster cluster;
    private Peers peers;
    private Gossips gossips;
    private Disposable disposable;
    private GossipProbe gossipProbe;
    private GossipTransport gossipTransport;

    // for testing
    public GossipProtocol(InnerNode node, Gossips gossips, GossipTransport gossipTransport, GossipOnPingDelay onPingDelay) {
        this.node = node;
        this.gossips = gossips;
        this.gossipTransport = gossipTransport;
        this.onPingDelay = onPingDelay;
    }

    public GossipProtocol(InnerNode node) {
        this.node = node;
        this.cluster = new Cluster(node.getNodeId());
        this.gossipTransport = new GossipTransport();
        this.peers = new Peers();
        this.gossips = Gossips.builder().nodeId(node.getNodeId()).maxGossips(maxGossips).build();
        this.onPingDelay = GossipOnPingDelay.NO_DELAY;
        this.gossipProbe = new GossipProbe(node.getNodeId(), gossipTransport);
    }

    public void dispose() {
        disposable.dispose();
    }

    public void start() {
        disposable = probeNodes().subscribe();
    }

    // visible for testing
    Flux<ProbeResult> probeNodes() {
        return Flux.defer(() -> {
                PeerProbe peerProbe = peers.nextPeerProbe(subgroupSize);
                if (peerProbe.getDestinationNodeId() == null) {
                    LOGGER.info("[Node {}][ping] No probing for one-node cluster", node.getNodeId());
                } else {
                    LOGGER.info("[Node {}][ping] Probing {} ...", node.getNodeId(), peerProbe.getDestinationNodeId());
                }
                return gossipProbe.probeNode(peerProbe, gossips.chooseLatestGossipsForPeer(), Mono.delay(indirectStart), Mono.delay(protocolPeriod));
            })
            .doOnNext(probeResult -> {
                gossips.probeCompleted(probeResult);
                LOGGER.info("[Node {}][ping] Probing {} finished.", node.getNodeId(), probeResult.getDestinationNodeId());
            })
            .repeat(REPEAT_PROBE)
            .doOnError(throwable -> LOGGER.error("[Node {}] Probe nodes job has been stopped!", node.getNodeId(), throwable));
    }

    public Mono<InitJoinResponse> join(InitJoinRequest initJoinRequest) {
        // trying to join other node
        JoinRequest joinRequest = JoinRequest.newBuilder().setPort(initJoinRequest.getPort()).setRequesterPort(initJoinRequest.getRequesterPort()).build();
        return Mono.defer(() -> GossipTcpTransport.join(joinRequest))
            .doOnNext(joinResponse -> {
                LOGGER.info("[Node {}] add member {}", node.getNodeId(), initJoinRequest.getPort());
                peers.add(initJoinRequest.getPort());
                cluster.addMember(initJoinRequest.getPort());
                node.nodeJoined(initJoinRequest.getPort());
            })
            .retryWhen(Retry
                    .onlyIf(objectRetryContext -> initJoinRequest.getRetry())
                    .fixedBackoff(Duration.ofSeconds(1))
                    .doOnRetry(context -> LOGGER.warn("[Node {}] Join {} failed. Retrying ...", node.getNodeId(), initJoinRequest.getPort()))
            )
            .thenReturn(InitJoinResponse.newBuilder().setStatus(true).build());
    }

    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        // other node asked me to join
        return Mono.just(joinRequest)
                   // TODO check credentials
                   .doOnNext(joinRequest1 -> {
                       LOGGER.info("[Node {}] add member {}", node.getNodeId(), joinRequest1.getRequesterPort());
                       peers.add(joinRequest1.getRequesterPort());
                       cluster.addMember(joinRequest1.getRequesterPort());
                       node.nodeJoined(joinRequest1.getRequesterPort());
                   })
                   .thenReturn(JoinResponse.newBuilder().setStatus(true).build());
    }

    public Mono<InitLeaveResponse> onInitLeaveRequest(InitLeaveRequest initLeaveRequest) {
        return null;
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
                        Ack ack = gossips.onPing(node.getNodeId(), ping);
                        DatagramPacket ackDatagram = AckUtils.toDatagram(ack, datagramPacket.sender());

                        Mono<?> publisher;
                        if (ping.getDirect()) {
                            publisher = onPingDelay.apply(ping.getRequestorNodeId(), ping.getCounter())
                                    .thenReturn(ackDatagram).cast(Object.class)
                                    .onErrorReturn(Mono.error(new GossipException("onPing direct ping error")));
                        } else {
                            Ping pingOnBehalf = PingUtils.direct(ping, ack.getGossipsList());
                            publisher = gossipTransport.ping(pingOnBehalf)
                                    .doOnNext(indirectAck -> {
                                        LOGGER.info("[Node {}][onPing] Probe to {} on behalf of {} successful.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
                                        gossips.addAck(indirectAck);
                                    })
                                    // TODO timeout
                                    .doOnError(throwable -> {
                                        LOGGER.warn("[Node {}][onPing] Probe to {} on behalf of {} failed. Reason {}.", pingOnBehalf.getRequestorNodeId(), pingOnBehalf.getDestinationNodeId(), pingOnBehalf.getInitiatorNodeId(), throwable.getMessage());
                                    })
                                    .thenReturn(ackDatagram)
                                    // if proxy is available and cannot reach destination then nack is returned
                                    .onErrorReturn(AckUtils.toDatagram(nack(ack), datagramPacket.sender()))
                                    .cast(Object.class)
                                    .onErrorReturn(Mono.error(new GossipException("onPing indirect ping error")));
                        }
                        return publisher.flatMap(ackDatagramOrMonoError -> {
                            if (ackDatagramOrMonoError instanceof Publisher) {
                                Mono monoError = (Mono) ackDatagramOrMonoError;
                                return udpOutbound.sendObject(monoError).then();
                            } else {
                                return udpOutbound.sendObject(ackDatagramOrMonoError).then();
                            }
                        });
                    } catch (GossipException e) {
                        return udpOutbound.sendObject(Mono.error(e)).then();
                    } catch (Exception e) {
                        String errorMessage = e.getMessage() != null ? e.getMessage() : "onPing unexpected error";
                        return udpOutbound.sendObject(Mono.error(new GossipException(errorMessage, e))).then();
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
        if (ping.getInitiatorNodeId() != ping.getRequestorNodeId()) {
            throw new GossipException(String.format("Initiator and requestor node must be the same! [%s,%s]", ping.getInitiatorNodeId(), ping.getRequestorNodeId()));
        }
        if (ping.getDestinationNodeId() != node.getNodeId()) {
            throw new GossipException(String.format("This node is not a destination node! [%s,%s]", ping.getDestinationNodeId(), node.getNodeId()));
        }
    }

    private void checkIndirectPing(Ping ping) {
        if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
            throw new GossipException(String.format("Initiator and requestor node must not be the same! [%s,%s]", ping.getInitiatorNodeId(), ping.getRequestorNodeId()));
        }
        if (ping.getRequestorNodeId() != node.getNodeId()) {
            throw new GossipException(String.format("This node is not a proxy node! [%s,%s]", ping.getRequestorNodeId(), node.getNodeId()));
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

}
