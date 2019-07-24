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
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class GossipTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTest.class);

    private static class GossipNode {

        private long probePeriod = 500;
        private long probeInterval = probePeriod * 3;

        private volatile long delay = 1000;

        private int nodeId;
        private int incarnationNumber;
        private Connection connection;
        private LoopResources loopResources;

        private List<Gossip> gossips = new CopyOnWriteArrayList<>();

        public GossipNode(int nodeId) {
            this.nodeId = nodeId;
            this.incarnationNumber = 0;
            this.loopResources = LoopResources.create("gossip-"+nodeId);
            this.connection = UdpServer.create()
                    .port(nodeId)
                    .handle(this::onPing)
                    .bindNow(Duration.ofSeconds(1));
        }

        Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
            return udpInbound.receiveObject()
                    .cast(DatagramPacket.class)
                    .flatMap(datagramPacket -> {
                        LOGGER.info("[Node {}] onPing", nodeId);
                        Ping ping = toPing2(datagramPacket);
                        Ack ack = Ack.newBuilder().setNodeId(2).build();
//                        if (ping.getNodeId() == nodeId) {
                        if (!ping.getReq()) {
                            Duration delayDur = Duration.ofMillis(delay);
                            delay = delay - 300;
                            LOGGER.info("[Node {}] DIRECT PROBE. Delay {}", nodeId, delayDur);
//                            return udpOutbound.sendByteArray(Mono.just(ack.toByteArray()));
                            return Mono.just(1)
                                        .delayElement(delayDur)
                                        .then(udpOutbound.sendObject(new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender()))
                                        .then());
                        } else {
                            LOGGER.info("[Node {}] INDIRECT PROBE to {}", nodeId, ping.getNodeId());

                            return client(ping.getNodeId())
                                        .flatMap(connection -> connection
                                                .outbound()
//                                                .sendByteArray(Mono.just(ping.toByteArray()))
                                                .sendObject(Unpooled.copiedBuffer(Ping.newBuilder(ping).setReq(false).build().toByteArray()))
                                                .then(connection.inbound()
                                                        .receiveObject()
                                                        .cast(DatagramPacket.class)
                                                        .map(this::toAck)
                                                        .next()
                                                        .timeout(Duration.ofMillis(probePeriod))
                                                        .flatMap(o -> udpOutbound
                                                                .sendObject(new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender()))
                                                                .then()
                                                        )
                                                        .onErrorResume(throwable -> {
                                                            LOGGER.info("[Node {}] InDirect probe to {} failed!", this.nodeId, ping.getNodeId());
                                                            return Mono.empty();
                                                        })
                                                ).then()
                                        );

                        }
                    })
//                    .delayElements(Duration.ofMillis(10))
                    .log("udp-server")
                    .then();
        }

        Publisher<Void> ping(int nodeId, Integer ... indirectNodeIds) {
            Ping ping = Ping.newBuilder().setNodeId(nodeId).build();
            return client(nodeId)
                .flatMap(clientConnection -> clientConnection
                    .outbound()
                    .sendByteArray(Mono.just(ping.toByteArray()))
                    .then(clientConnection.inbound()
                            .receiveObject()
                            .doOnNext(i -> LOGGER.info("[Node {}] receiving ack from {}", this.nodeId, nodeId))
                            .cast(DatagramPacket.class)
                            .map(this::toAck)
                            .next()
                            .timeout(Duration.ofMillis(probePeriod))
                            .then()
                    )
                    .then()
                    .onErrorResume(throwable -> {
                        LOGGER.info("[Node {}] Direct probe to {} failed!", this.nodeId, nodeId);

                        return Flux.fromArray(indirectNodeIds)
                                .flatMap(this::client)
                                .flatMap(connection2 -> connection2
                                .outbound()
//                                .sendByteArray(Mono.just(Ping.newBuilder(ping).setReq(true).build().toByteArray()))
                                .sendObject(Unpooled.copiedBuffer(Ping.newBuilder(ping).setReq(true).build().toByteArray()))
                                .then(connection2.inbound()
                                .receiveObject()
                                .doOnNext(i -> LOGGER.info("[Node {}] {} receiving ack from {}", this.nodeId, connection2.address().getPort(), nodeId))
                                .cast(DatagramPacket.class)
                                .map(this::toAck)
                                .next()
                                .timeout(Duration.ofMillis(probeInterval - probePeriod))
                                .onErrorResume(throwable1 -> {
                                    LOGGER.info("[Node {}] {} failed {}", this.nodeId, connection2.address().getPort(), nodeId);
                                    return Mono.empty();
                                })
                                .flatMap(o -> clientConnection
                                                .outbound()
                                                .sendByteArray(Mono.just(Ack.newBuilder().build().toByteArray()))
                                                .then()
                                        )
                                )
                            )
                            .then();
                    }))
                    .log();
//                    .timeout(Duration.ofMillis(probeInterval));
        }

        private Ping toPing2(DatagramPacket datagramPacket) {
            try {
                Ping ping = Ping.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
                LOGGER.info("[Node {}] Ping received {}", nodeId, ping);
                return ping;
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }

        private DatagramPacket toAck(DatagramPacket datagramPacket) {
            LOGGER.info("[Node {}] ack ", nodeId);

            Ack ack = Ack.newBuilder()
                    .addGossips(Gossip.newBuilder().setIncarnation(32).build())
                    .build();
            return new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), datagramPacket.sender());
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

        Flux.from(node1.ping(7001, 7002, 7003)).blockLast();

        Thread.sleep(1_000);

//        node1.disposeNow();
//        node2.disposeNow();
//        node3.disposeNow();
    }

}
