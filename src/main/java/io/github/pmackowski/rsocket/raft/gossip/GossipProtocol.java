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
import java.util.List;
import java.util.function.BooleanSupplier;

import static io.github.pmackowski.rsocket.raft.gossip.GossipLogger.*;
import static io.github.pmackowski.rsocket.raft.gossip.PingUtils.toPing;

public class GossipProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

    private Duration indirectStart = Duration.ofMillis(400);
    private Duration protocolPeriod = Duration.ofSeconds(2);
    private int subgroupSize = 2;

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
    public GossipProtocol(InnerNode node, Gossips gossips, GossipOnPingDelay onPingDelay) {
        this.node = node;
        this.gossips = gossips;
        this.onPingDelay = onPingDelay;
    }

    public GossipProtocol(InnerNode node) {
        this.node = node;
        this.cluster = new Cluster(node.getNodeId());
        this.gossipTransport = new GossipTransport();
        this.peers = new Peers();
        this.gossips = new Gossips(node.getNodeId());
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
    Flux<Acks> probeNodes() {
        return Flux.defer(() -> {
                PeerProbe peerProbe = peers.nextPeerProbe(subgroupSize);
                log(node.getNodeId(), peerProbe);
                return gossipProbe.probeNode(peerProbe, gossips.share(), Mono.delay(indirectStart), Mono.delay(protocolPeriod));
            })
//                .doOnNext(acks -> this.gossips.addGossips(acks))
            .doOnNext(acks -> {
                //LOGGER.info("[Node {}][ping] Probing {} finished.", node.getNodeId(), acks);
            })
            .doOnError(throwable -> {
//                    this.gossips.addGossip(destinationNodeId, Gossip.Suspicion.SUSPECT);
            })
            .doFinally(signalType -> {
                //                Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
                //                this.gossips.addGossip(nodeId, suspicion);
            })
            .repeat(REPEAT_PROBE)
            .doOnError(throwable -> LOGGER.error("--",throwable));
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
                    Ping ping = toPing(datagramPacket);
                    logOnPing(ping);
                    List<Gossip> sharedGossips = gossips.mergeAndShare(ping.getGossipsList());
                    Ack ack = Ack.newBuilder().setNodeId(node.getNodeId()).addAllGossips(sharedGossips).build();

                    Publisher<?> publisher;
                    if (ping.getDirect()) {
                        publisher = onPingDelay.apply(ping.getRequestorNodeId(), ping.getCounter());
                    } else {
                        Ping newPing = PingUtils.direct(ping, sharedGossips);
                        publisher = gossipTransport.ping(newPing)
                                .doOnNext(i -> log(ping))
                                .onErrorResume(throwable -> {
                                    logError(ping, throwable);
                                    return Mono.empty();
                                }); // TODO handle ack and timeout
                    }
                    return Flux.from(publisher).then(udpOutbound.sendObject(AckUtils.toDatagram(ack, datagramPacket.sender())).then());
                })
                .then();
    }

}
