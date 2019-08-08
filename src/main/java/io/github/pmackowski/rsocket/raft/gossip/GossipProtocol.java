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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static io.github.pmackowski.rsocket.raft.gossip.PingUtils.toPing;

public class GossipProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

    private Duration indirectStart = Duration.ofMillis(400);
    private Duration protocolPeriod = Duration.ofSeconds(2);
    private int subgroupSize = 2;

    private AtomicLong protocolPeriodCounter = new AtomicLong(0);
    private GossipOnPingDelay onPingDelay;
    private BooleanSupplier REPEAT_PROBE = () -> true;

    int nodeId;

    private InnerNode node;
    private Cluster cluster;
    private Peers peers;
    private Gossips gossips;
    private Disposable disposable;

    public GossipProtocol(InnerNode node) {
        this.node = node;
        this.cluster = new Cluster(node.getNodeId());
        this.nodeId = node.getNodeId();
        this.peers = new Peers();
        this.gossips = new Gossips(nodeId);
        this.onPingDelay = GossipOnPingDelay.NO_DELAY;
    }

    public GossipProtocol(int nodeId) {
        this(nodeId, GossipOnPingDelay.NO_DELAY);
    }

    public GossipProtocol(int nodeId, GossipOnPingDelay onPingDelay) {
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
//                .doOnNext(acks -> this.gossips.addGossips(acks))
                .doOnNext(acks -> {
                    LOGGER.info("[Node {}][ping] Probing {} finished.", this.nodeId, acks);
                })
                .doOnError(throwable -> {
//                    this.gossips.addGossip(destinationNodeId, Gossip.Suspicion.SUSPECT);
                })
                .doFinally(signalType -> {
        //                Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        //                this.gossips.addGossip(nodeId, suspicion);
                })

                .repeat(REPEAT_PROBE)
                .doOnError(throwable -> LOGGER.error("--",throwable))
                .subscribe();
    }

    private Flux<Acks> probeNode() {
        GossipProbe gossipProbe = new GossipProbe(this);
        return Flux.defer(() -> {
            protocolPeriodCounter.incrementAndGet();
            Integer destinationNodeId = peers.nextRandomPeerId();
            if (destinationNodeId == null) {
                return Mono.delay(protocolPeriod).thenReturn(new Acks());
            }
            List<Integer> proxyNodeIds = peers.selectCompanions(destinationNodeId, subgroupSize);
            return gossipProbe.probeNode(destinationNodeId, proxyNodeIds, gossips.share(), Mono.delay(indirectStart), Mono.delay(protocolPeriod));
        });
    }

    public Mono<InitJoinResponse> join(InitJoinRequest initJoinRequest) {
        // trying to join other node
        JoinRequest joinRequest = JoinRequest.newBuilder().setPort(initJoinRequest.getPort()).setRequesterPort(initJoinRequest.getRequesterPort()).build();
        return Mono.defer(() -> GossipTcpTransport.join(joinRequest))
            .doOnNext(joinResponse -> peers.add(initJoinRequest.getPort()))
            .doOnNext(joinResponse -> {
                LOGGER.info("[Node {}] add member {}", nodeId, initJoinRequest.getPort());
                cluster.addMember(initJoinRequest.getPort());
                node.nodeJoined(initJoinRequest.getPort());
            })
            .retryWhen(Retry
                    .onlyIf(objectRetryContext -> initJoinRequest.getRetry())
                    .fixedBackoff(Duration.ofSeconds(1))
                    .doOnRetry(context -> LOGGER.warn("[Node {}] Join {} failed. Retrying ...", nodeId, initJoinRequest.getPort()))
            )
            .thenReturn(InitJoinResponse.newBuilder().setStatus(true).build());
    }

    public Mono<JoinResponse> onJoinRequest(JoinRequest joinRequest) {
        // other node asked me to join
//        senders.addServer(joinRequest1.getPort());
        return Mono.just(joinRequest)
                   // TODO check credentials
                   .doOnNext(joinRequest1 -> peers.add(joinRequest1.getRequesterPort()))
                   .doOnNext(joinRequest1 -> {
                       LOGGER.info("[Node {}] add member {}", nodeId, joinRequest1.getRequesterPort());
                       cluster.addMember(joinRequest1.getRequesterPort());
                       node.nodeJoined(joinRequest1.getRequesterPort());
                   })
                   .thenReturn(JoinResponse.newBuilder().setStatus(true).build());
    }

    public Mono<InitLeaveResponse> onInitLeaveRequest(InitLeaveRequest initLeaveRequest) {
        return null;
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
