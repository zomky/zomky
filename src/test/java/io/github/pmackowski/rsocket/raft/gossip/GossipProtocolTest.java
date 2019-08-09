package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyOutbound;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GossipProtocolTest {

    private static final int SENDER_NODE_ID = 7000;
    private static final int RECIPIENT_NODE_ID = 7001;
    private static final InetSocketAddress SENDER = InetSocketAddress.createUnresolved("localhost", SENDER_NODE_ID);
    private static final InetSocketAddress RECIPIENT = InetSocketAddress.createUnresolved("localhost", RECIPIENT_NODE_ID);

    @Mock InnerNode node;
    @Mock Gossips gossips;
    @Mock UdpInbound udpInbound;
    @Mock UdpOutbound udpOutbound;
    @Spy NettyOutbound nettyOutbound;

    private GossipProtocol gossipProtocol;

    @BeforeEach
    void setUp() {
        BDDMockito.given(node.getNodeId()).willReturn(SENDER_NODE_ID);
        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
    }

    @Test
    void onPing() {
        // given
        Ping ping = Ping.newBuilder()
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);
        BDDMockito.given(gossips.mergeAndShare(ping.getGossipsList())).willReturn(new ArrayList<>());

        gossipProtocol = new GossipProtocol(node, gossips, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                    .expectSubscription()
                    .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(node.getNodeId())
                .addAllGossips(new ArrayList<>())
                .build()
        );
    }

    private void givenPing(Ping ping) {
        DatagramPacket datagramPacket = new DatagramPacket(Unpooled.copiedBuffer(ping.toByteArray()), RECIPIENT, SENDER);
        BDDMockito.<Flux<?>>given(udpInbound.receiveObject()).willReturn(Flux.just(datagramPacket));
    }

    private void assertAck(Ack ack) {
        ArgumentCaptor<DatagramPacket> datagramPacketCapture = ArgumentCaptor.forClass(DatagramPacket.class);
        verify(udpOutbound).sendObject(datagramPacketCapture.capture());
        DatagramPacket datagramPacket = datagramPacketCapture.getValue();
        assertThat(datagramPacket.content()).isEqualTo(Unpooled.copiedBuffer(ack.toByteArray()));
//        assertThat(datagramPacket.sender()).isEqualTo(SENDER);
    }
}