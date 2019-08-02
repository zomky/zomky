package io.github.pmackowski.rsocket.raft.integration.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.udp.UdpClient;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.netty.udp.UdpServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class GossipNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipNode.class);

    private long probeTimeout = 500;
    private long protocolPeriod = probeTimeout * 3;

    private List<Integer> nodes = new ArrayList<>();

    int nodeId;
    private BiFunction<Integer,Integer, Duration> delayFunction;
    private Gossips gossips;
    private LoopResources loopResources;
    private Connection connection;
    private Disposable disposable;
    private AtomicInteger delayCounter = new AtomicInteger(0);

    public GossipNode(int nodeId) {
        this(nodeId, (otherNodeId, counter) -> Duration.ZERO);
    }

    public GossipNode(int nodeId, BiFunction<Integer,Integer, Duration> delayFunction) {
        this.nodeId = nodeId;
        this.delayFunction = delayFunction;
        this.loopResources = LoopResources.create("gossip-"+nodeId);
        this.gossips = new Gossips(nodeId);
        this.connection = UdpServer.create()
                .port(nodeId)
                .handle(this::onPing)
                .bindNow(Duration.ofSeconds(1));
    }

    public void disposeNow() {
        connection.disposeNow();
        disposable.dispose();
    }

    public void start() {
//        Queue<Integer> nodesInRandomOrder = new ArrayList<>();
//        int selectedPeer = nodesInRandomOrder.poll();

        GossipProbe gossipProbe = new GossipProbe(this);

        disposable = Flux.defer(() -> {
                    int destinationNodeId = 7001; // round-robin from random list
                    List<Integer> proxyNodeIds = Arrays.asList(7002, 7003);
                    List<Gossip> gossips = new ArrayList<>();
                    Publisher<?> indirectStart = Mono.delay(Duration.ofMillis(400));
                    Publisher<?> protocolPeriodEnd = Mono.delay(Duration.ofSeconds(2));
                    return gossipProbe.probeNode(destinationNodeId, proxyNodeIds, gossips, indirectStart, protocolPeriodEnd);
                })
                .doOnNext(ack -> {
                    LOGGER.info("Acks {}", ack);
                })
                .doFinally(signalType -> {
        //                Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        //                this.gossips.addGossip(nodeId, suspicion);
                    LOGGER.info("[Node {}][ping] Probing {} finished.", this.nodeId, 2313123);
                })
                .repeat()
                .subscribe();

        List<Integer> indirectPeers = new ArrayList<>(); // select configurable amount of peers
    }

    Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
        return udpInbound.receiveObject()
                .cast(DatagramPacket.class)
                .flatMap(datagramPacket -> {
                    Ping ping = toPing(datagramPacket);
                    List<Gossip> sharedGossips = gossips.mergeAndShare(ping.getGossipsList());
                    Ack ack = Ack.newBuilder().setNodeId(22).addAllGossips(sharedGossips).build();

                    if (ping.getDirect()) {
                        Duration delay = delayFunction.apply(ping.getRequestorNodeId(), delayCounter.incrementAndGet());
                        if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
                            LOGGER.info("[Node {}][onPing] I am being probed by {} , my delay {}", nodeId, ping.getRequestorNodeId(), delay);
                        } else {
                            LOGGER.info("[Node {}][onPing] I am being probed by {} on behalf of {}, my delay {}", nodeId, ping.getRequestorNodeId(), ping.getInitiatorNodeId(), delay);
                        }
                        return Mono.just(1)
                                .delayElement(delay)
                                .then(udpOutbound.sendObject(new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender())).then());
                    } else {
                        LOGGER.info("[Node {}][onPing] Probing {} on behalf of {}", nodeId, ping.getDestinationNodeId(), ping.getInitiatorNodeId());
                        GossipTransport gossipTransport = new GossipTransport();
                        return client(ping.getDestinationNodeId())
                                .flatMap(connection -> connection
                                                .outbound()
                                                // which gossips should be sent ??
                                                .sendObject(Unpooled.copiedBuffer(Ping.newBuilder(ping).clearGossips().addAllGossips(gossips.share()).setDirect(true).build().toByteArray()))
                                                .then(connection.inbound()
                                                        .receiveObject()
                                                        .cast(DatagramPacket.class)
                                                        .next()
                                                        .doOnNext(i -> LOGGER.info("[Node {}][onPing] Probing {} on behalf of {} successful.", nodeId, ping.getDestinationNodeId(), ping.getInitiatorNodeId()))
                                                        .doOnNext(datagramPacket1 -> addGossips(datagramPacket1, nodeId))
                                                        .timeout(Duration.ofMillis(probeTimeout))
                                                        .flatMap(datagramPacket1 -> udpOutbound
                                                                .sendObject(new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender()))
                                                                .then()
                                                        )
                                                        .onErrorResume(throwable -> {
                                                            LOGGER.info("[Node {}][onPing] Probing {} on behalf of {} failed.", nodeId, ping.getDestinationNodeId(), ping.getInitiatorNodeId());
                                                            return Mono.empty();
                                                        })
                                                ).then()
                                );
                    }
                })
                .then();
    }

    private Ping toPing(DatagramPacket datagramPacket) {
        LOGGER.debug("[Node {}] toPing Datagram receipient{}", nodeId, datagramPacket.recipient());
        LOGGER.debug("[Node {}] toPing Datagram sender {}", nodeId, datagramPacket.sender());
        try {
            return Ping.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private void addGossips(DatagramPacket datagramPacket, int nodeId) {
        Ack ack = toAck(datagramPacket);
        gossips.mergeAndShare(ack.getGossipsList());
    }

    private Ack toAck(DatagramPacket datagramPacket) {
        LOGGER.debug("[Node {}] toAck Datagram receipient{}", nodeId, datagramPacket.recipient());
        LOGGER.debug("[Node {}] toAck Datagram sender {}", nodeId, datagramPacket.sender());
        try {
            return Ack.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<? extends Connection> client(int port) {
        return UdpClient.create()
                .port(port)
                .runOn(loopResources)
                .connect()
                .doOnSubscribe(subscription -> {
                    LOGGER.debug("[Node {}] Probe {} subscription", nodeId, port);
                })
                .doOnCancel(() -> {
                    LOGGER.info("[Node {}] Indirect probe {} cancelled", nodeId, port);
                })
                .doOnError(throwable -> {
                    LOGGER.warn("Cannont connect!");
                });
    }
}
