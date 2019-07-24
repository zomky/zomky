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

public class GossipTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTest.class);

    private static LoopResources loopResources = LoopResources.create("echo-test-udp");

    private static class GossipNode {

       private int incarnationNumber;

       Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
            return udpInbound.receiveObject()
                    .doOnNext(o -> LOGGER.info("OnPing received"))
                    .map(o -> { // Transforms the datagram.
                        if (o instanceof DatagramPacket) {
                            DatagramPacket p = (DatagramPacket) o;

                            try {
                                final Ping ping = Ping.parseFrom(NettyUtils.toByteArray(p.content().retain()));
                                LOGGER.info("Received ping {}", ping);
                                Ack ack = Ack.newBuilder()
                                        .addGossips(Gossip.newBuilder().setIncarnation(32).build())
                                        .build();
                                return new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), p.sender());
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        else {
                            return Mono.error(new Exception("Unexpected type of the message: " + o));
                        }
                    })
//                    .delayElements(Duration.ofMillis(200))
                    .flatMap(udpOutbound::sendObject)
                    .log("udp-server")
                    .then();
        }

        Publisher<Void> pingReq(UdpOutbound udpOutbound) {
            return Mono.empty();
        }

        Publisher<Void> ack(UdpInbound udpInbound, UdpOutbound udpOutbound) {
            return Mono.empty();
        }

    }

    @Test
    public void echoTest() throws Exception {
        GossipNode node = new GossipNode();

        Connection server1 = server(node, 7000);
        Connection server2 = server(node, 7001);
        Connection server3 = server(node, 7002);



        Connection client = client(7000);
        final Ping ping = Ping.newBuilder()
                .addGossips(Gossip.newBuilder().setIncarnation(1).setNodeId(7000).setOriginNodeId(7001).setSuspicion(Gossip.Suspicion.SUSPECT).build())
                .addGossips(Gossip.newBuilder().setIncarnation(0).setNodeId(7002).setOriginNodeId(7001).setSuspicion(Gossip.Suspicion.CONFIRM).build())
                .build();

        client.outbound()
                .sendByteArray(Mono.just(ping.toByteArray()))
                .then(client.inbound()
                        .receiveObject()
                        .cast(DatagramPacket.class)
                        .next()
                        .timeout(Duration.ofMillis(100))
                        .doOnNext(this::processAck)
                        .then()
                )
                .then()
                .onErrorResume(throwable -> {
                    Flux<Connection> clientReqs = Flux.just(client(7001), client(7002));
                    final Ping ping2 = Ping.newBuilder()
                            .addGossips(Gossip.newBuilder().setIncarnation(110).setNodeId(27002).build())
                            .build();
                    return clientReqs.flatMap(connection -> connection.outbound()
                                          .sendByteArray(Mono.just(ping2.toByteArray()))
                                          .then(ddd(connection)))
                                    .then();
                })
                .subscribe();

        Thread.sleep(5_000);

        server1.disposeNow();
        server2.disposeNow();

        client.disposeNow();
    }

    private void processAck(DatagramPacket p) {
        try {
            final Ack ack = Ack.parseFrom(NettyUtils.toByteArray(p.content().retain()));
            LOGGER.info("Received ack {}", ack);

        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    Mono<Void> ddd(Connection clientReq) {
        return clientReq.inbound()
                .receiveObject()
                .next()
                .doOnNext(o -> {
                    if (o instanceof DatagramPacket) {
                        DatagramPacket p = (DatagramPacket) o;
                        try {
                            final Ack ack = Ack.parseFrom(NettyUtils.toByteArray(p.content().retain()));
                            LOGGER.info("Received ack {}", ack);
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .then();
    }

    Connection server(GossipNode node, int port) {
        return UdpServer.create()
                .port(port)
                .runOn(loopResources)
                .handle(node::onPing)
                .bindNow(Duration.ofSeconds(1));
    }

    Connection client(int port) {
        return UdpClient.create()
                .port(port)
                .runOn(loopResources)
                .connectNow(Duration.ofSeconds(1));
    }

}
