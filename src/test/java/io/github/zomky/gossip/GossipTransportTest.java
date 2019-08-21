package io.github.zomky.gossip;

import io.github.zomky.gossip.protobuf.Ack;
import io.github.zomky.gossip.protobuf.Ping;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.netty.udp.UdpServer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.Function;

import static io.github.zomky.gossip.PingUtils.toPing;
import static org.assertj.core.api.Assertions.assertThat;

class GossipTransportTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTransportTest.class);

    private static final int INITIATOR_NODE_ID = 7000;
    private static final int DESTINATION_NODE_ID = 7001;

    private GossipTransport gossipTransport = new GossipTransport();
    private Connection gossipReceiver;

    @AfterEach
    void tearDown() {
        gossipReceiver.disposeNow();
    }

    @Test
    void pingSuccessful() {
        udpServer(datagramPacket -> {
            Ping ping = toPing(datagramPacket);
            LOGGER.info("[Node {}][onPing] I am being probed by {}", ping.getDestinationNodeId(), ping.getRequestorNodeId());
            return Ack.newBuilder().setNodeId(123).build();
        });

        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(INITIATOR_NODE_ID)
                .setRequestorNodeId(INITIATOR_NODE_ID)
                .setDestinationNodeId(DESTINATION_NODE_ID)
                .setDirect(true)
                .build();

        StepVerifier.create(pingUdpServer(ping))
                .assertNext(ack -> assertThat(ack.getNodeId()).isEqualTo(123))
                .thenCancel()
                .verify();
    }

    @Test
    @Disabled
    void pingError() {
        udpServer(datagramPacket -> {
            Ping ping = toPing(datagramPacket);
            LOGGER.info("[Node {}][onPing] I am being probed by {}", ping.getDestinationNodeId(), ping.getRequestorNodeId());
            // really weird but it is taken from reactor netty guide
            return Mono.error(new RuntimeException());
        });

        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(INITIATOR_NODE_ID)
                .setRequestorNodeId(INITIATOR_NODE_ID)
                .setDestinationNodeId(DESTINATION_NODE_ID)
                .setDirect(true)
                .build();

        StepVerifier.create(pingUdpServer(ping))
                .assertNext(ack -> assertThat(ack.getNodeId()).isEqualTo(123))
                .verifyComplete();
    }

    private void udpServer(Function<DatagramPacket, Object> toAckFunction) {
        gossipReceiver = UdpServer.create()
                .port(DESTINATION_NODE_ID + 20000)
                .handle((udpInbound, udpOutbound) -> onPing(udpInbound, udpOutbound, toAckFunction))
                .bindNow(Duration.ofSeconds(1));
    }

    private Flux<Ack> pingUdpServer(Ping ping) {
        return gossipTransport
                .ping(ping)
                .doOnSubscribe(s-> LOGGER.info("[Node {}] Probing {} ...", ping.getInitiatorNodeId(), ping.getDestinationNodeId()));
    }

    private Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound, Function<DatagramPacket, Object> toAckFunction) {
        return udpOutbound.sendObject(
                    udpInbound.receiveObject()
                        .cast(DatagramPacket.class)
                        .map(datagramPacket -> {
                            Object obj = toAckFunction.apply(datagramPacket);
                            if (obj instanceof Ack) {
                                return AckUtils.toDatagram((Ack) obj, datagramPacket.sender());
                            } else {
                                return obj;
                            }
                        })
                );
    }
}