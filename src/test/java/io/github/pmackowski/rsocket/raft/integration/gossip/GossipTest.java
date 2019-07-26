package io.github.pmackowski.rsocket.raft.integration.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.udp.UdpClient;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.netty.udp.UdpServer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GossipTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTest.class);

    private static class Gossips {

        private int nodeId;
        private int incarnation;
        Map<Integer, Gossip> gossips = new ConcurrentHashMap<>();

//        Queue<Gossip> gossips = new LinkedBlockingQueue<>();

        public Gossips(int nodeId) {
            this.nodeId = nodeId;
        }

        public synchronized void addGossip(int nodeId, Gossip.Suspicion suspicion) {
            addGossip(nodeId, incarnation, suspicion);
        }

        public synchronized void addGossip(int nodeId, int incarnation, Gossip.Suspicion suspicion) {
            // Node is being suspected of being faulty
            if (this.nodeId == nodeId) {
                if (this.incarnation == incarnation && suspicion == Gossip.Suspicion.SUSPECT) {
                    this.incarnation++;
                    gossips.put(this.nodeId, gossip(this.nodeId, Gossip.Suspicion.ALIVE));
                }
                return;
            }

            Gossip gossip = gossips.get(nodeId);
            if (gossip != null && gossip.getSuspicion() == Gossip.Suspicion.DEAD) {
                return;
            }

            if (suspicion == Gossip.Suspicion.ALIVE) {
                if (gossip == null) {
                    gossips.put(nodeId, gossip(nodeId, suspicion));
                } else if (gossip.getIncarnation() ){

                }
            }

            if (suspicion == Gossip.Suspicion.DEAD && gossip.getSuspicion() != Gossip.Suspicion.DEAD) {
                gossips.put(nodeId, gossip(nodeId, suspicion));
                return;
            }

            else {
                if (suspicion == Gossip.Suspicion.DEAD) {
                    gossips.put(nodeId, gossip());
                } else if (suspicion == Gossip.Suspicion.ALIVE) {

                }

//                gossip.getIncarnation()
            }
        }

        private Gossip gossip(int nodeId, Gossip.Suspicion suspicion) {
            return Gossip.newBuilder().setNodeId(nodeId).setSuspicion(suspicion).setIncarnation(incarnation).build();
        }

        public List<Gossip> choose() {
            return null;
        }

        public List<Gossip> mergeAndShare(List<Gossip> gossipsList) {
            return null;
        }

        public List<Gossip> share() {
            return null;
        }

        private Gossip chooseBetween(Gossip gossip1, Gossip gossip2) {
            return null;
        }
    }

    private static class GossipNode {

        private long probeTimeout = 500;
        private long protocolPeriod = probeTimeout * 3;

        private volatile long delay = 1000;

        private int nodeId;
        private Gossips gossips;
        private LoopResources loopResources;
        private Connection connection;

        public GossipNode(int nodeId) {
            this.nodeId = nodeId;
            this.loopResources = LoopResources.create("gossip-"+nodeId);
            this.gossips = new Gossips(nodeId);
            this.connection = UdpServer.create()
                    .port(nodeId)
                    .handle(this::onPing)
                    .bindNow(Duration.ofSeconds(1));
        }

        Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
            return udpInbound.receiveObject()
                    .cast(DatagramPacket.class)
                    .flatMap(datagramPacket -> {
                        Ping ping = toPing(datagramPacket);
                        List<Gossip> sharedGossips = gossips.mergeAndShare(ping.getGossipsList());
                        Ack ack = Ack.newBuilder().addAllGossips(sharedGossips).build();

                        if (ping.getDirect()) {
                            Duration delayDur = Duration.ofMillis(delay);
                            delay = Math.max(0, delay - 600);
                            if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
                                LOGGER.info("[Node {}][onPing] I am being probed by {} , my delay {}", nodeId, ping.getRequestorNodeId(), delayDur);
                            } else {
                                LOGGER.info("[Node {}][onPing] I am being probed by {} on behalf of {}, my delay {}", nodeId, ping.getRequestorNodeId(), ping.getInitiatorNodeId(), delayDur);
                            }
                            return Mono.just(1)
                                        .delayElement(delayDur)
                                        .then(udpOutbound.sendObject(new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender())).then());
                        } else {
                            LOGGER.info("[Node {}][onPing] Probing {} on behalf of {}", nodeId, ping.getDestinationNodeId(), ping.getInitiatorNodeId());

                            return client(ping.getDestinationNodeId())
                                        .flatMap(connection -> connection
                                                .outbound()
                                                // which gossips should be sent ??
                                                .sendObject(Unpooled.copiedBuffer(Ping.newBuilder(ping).clearGossips().addAllGossips(gossips.share()).setDirect(true).build().toByteArray()))
//                                                .sendObject(Unpooled.copiedBuffer(Ping.newBuilder(ping).setDirect(true).build().toByteArray()))
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
//                    .delayElements(Duration.ofMillis(10))
//                    .log("udp-server")
                    .then();
        }

        Mono<Void> ping(int nodeId, Integer ... indirectNodeIds) {
            AtomicInteger acks = new AtomicInteger(0);
            Ping ping = Ping.newBuilder().setInitiatorNodeId(this.nodeId)
                    .setRequestorNodeId(this.nodeId)
                    .setDestinationNodeId(nodeId)
                    .addAllGossips(this.gossips.share())
                    .setDirect(true).build();
            LOGGER.info("[Node {}][ping] Probing {} ...", this.nodeId, nodeId);
            return client(nodeId)
                .flatMap(clientConnection -> clientConnection
                    .outbound()
                    .sendByteArray(Mono.just(ping.toByteArray()))
                    .then(clientConnection.inbound()
                            .receiveObject()
                            .cast(DatagramPacket.class)
                            .next()
                            .doOnNext(i -> LOGGER.info("[Node {}][ping] Direct probe to {} successful.", this.nodeId, nodeId))
                            .doOnNext(datagramPacket -> addGossips(datagramPacket, nodeId))
                            .timeout(Duration.ofMillis(probeTimeout))
                            .then()
                    )
                    .then()
                    .doOnNext(v -> acks.incrementAndGet())
                    .onErrorResume(throwable -> {
                        LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", this.nodeId, nodeId, throwable.getMessage());
                        LOGGER.info("[Node {}][ping] Trying indirect probes to {} through {}.", this.nodeId, nodeId, Arrays.asList(indirectNodeIds));

                        return Flux.fromArray(indirectNodeIds)
                                .flatMap(this::client)
                                .flatMap(connection2 -> connection2
                                    .outbound()
                                    .sendByteArray(Mono.just(Ping.newBuilder(ping).setRequestorNodeId(connection2.address().getPort()).setDirect(false).build().toByteArray()))
                                    .then(connection2.inbound()
                                        .receiveObject()
                                        .cast(DatagramPacket.class)
                                        .next()
                                        .doOnNext(i -> LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", this.nodeId, nodeId, connection2.address().getPort()))
                                        .doOnNext(datagramPacket -> addGossips(datagramPacket, nodeId))
                                        .doOnNext(v -> acks.incrementAndGet())
                                        .timeout(Duration.ofMillis(protocolPeriod - probeTimeout))
                                        .onErrorResume(throwable1 -> {
                                            LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}.", this.nodeId, nodeId, connection2.address().getPort(), throwable1.getMessage());
                                            return Mono.empty();
                                        })
                                        .flatMap(datagramPacket1 -> clientConnection
                                                        .outbound()
                                                        // TODO gossips
                                                        .sendObject(new DatagramPacket(Unpooled.copiedBuffer(Ack.newBuilder().build().toByteArray()), datagramPacket1.sender()))
                                                        .then()
                                        )
                                    )
                                )
                                .then();
                    })
                ).doFinally(signalType -> {
                    Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
                    this.gossips.addGossip(nodeId, suspicion);
                    LOGGER.info("[Node {}][ping] Probing {} finished. Acks {}", this.nodeId, nodeId, acks.get());
                });
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
                    .connect();
        }

        public void disposeNow() {
            connection.disposeNow();
        }
    }

    @Test
    public void echoTest() throws Exception {
        GossipNode node1 = new GossipNode(7000);
        GossipNode node2 = new GossipNode(7001);
        GossipNode node3 = new GossipNode(7002);
        GossipNode node4 = new GossipNode(7003);

        Thread.sleep(1_000);

        node1.ping(7001, 7002, 7003).block();
//        node3.ping(7000, 7001, 7003).subscribe();
//        node1.ping(7001).block();

        Thread.sleep(1_000);

//        node1.disposeNow();
//        node2.disposeNow();
//        node3.disposeNow();
    }

}
