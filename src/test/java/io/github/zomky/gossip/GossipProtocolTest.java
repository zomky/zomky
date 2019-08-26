package io.github.zomky.gossip;

import io.github.zomky.InnerNode;
import io.github.zomky.gossip.protobuf.Ack;
import io.github.zomky.gossip.protobuf.Gossip;
import io.github.zomky.gossip.protobuf.Ping;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
//@MockitoSettings(strictness = Strictness.LENIENT)
class GossipProtocolTest {

    private static final int NUMBER_OF_PEERS = 4;
    private static final int SENDER_NODE_ID = 7000;
    private static final int RECIPIENT_NODE_ID = 7001;
    private static final int PROXY_NODE_ID = 7002;
    private static final InetSocketAddress SENDER = InetSocketAddress.createUnresolved("localhost", SENDER_NODE_ID);
    private static final InetSocketAddress RECIPIENT = InetSocketAddress.createUnresolved("localhost", RECIPIENT_NODE_ID);

    @Mock InnerNode node;
    @Mock Peers peers;
    @Mock Gossips gossips;
    @Mock RandomGossipProbe randomGossipProbe;
    @Mock GossipTransport gossipTransport;
    @Mock UdpInbound udpInbound;
    @Mock UdpOutbound udpOutbound;
    @Mock NettyOutbound nettyOutbound;

    private GossipProtocol gossipProtocol;

    @BeforeEach
    void setUp() {
        BDDMockito.lenient().when(nettyOutbound.then()).thenReturn(Mono.empty());
        BDDMockito.lenient().when(peers.count()).thenReturn(NUMBER_OF_PEERS);
    }

    @Test
    void probeNodes() {
        // given
        ProbeResult probeResult1 = mock(ProbeResult.class);
        ProbeResult probeResult2 = mock(ProbeResult.class);
        ProbeResult probeResult3 = mock(ProbeResult.class);
        BDDMockito.given(randomGossipProbe.randomProbe())
                .willReturn(Mono.just(probeResult1))
                .willReturn(Mono.just(probeResult2))
                .willReturn(Mono.just(probeResult3));

        BDDMockito.given(gossips.probeInterval())
                .willReturn(Duration.ofMillis(1000))
                .willReturn(Duration.ofMillis(2000));

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.withVirtualTime(() -> gossipProtocol.probeNodes().log())
                    .expectSubscription()
                    .expectNext(probeResult1)
                    .expectNoEvent(Duration.ofMillis(1000))
                    .expectNext(probeResult2)
                    .expectNoEvent(Duration.ofMillis(2000))
                    .expectNext(probeResult3)
                    .thenCancel()
                    .verify();

        verify(randomGossipProbe).probeCompleted(probeResult1);
        verify(randomGossipProbe).probeCompleted(probeResult2);
        verify(randomGossipProbe).probeCompleted(probeResult3);
        verify(gossips, times(2)).probeInterval();
    }

    @Test
    void probeNodesError() {
        // given
        ProbeResult probeResult1 = mock(ProbeResult.class);
        BDDMockito.given(randomGossipProbe.randomProbe())
                .willReturn(Mono.just(probeResult1))
                .willReturn(Mono.error(new RuntimeException()));

        BDDMockito.given(gossips.probeInterval()).willReturn(Duration.ofMillis(1000));

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.withVirtualTime(() -> gossipProtocol.probeNodes().log())
                .expectSubscription()
                .expectNext(probeResult1)
                .expectNoEvent(Duration.ofMillis(1000))
                .verifyError(RuntimeException.class);

        verify(randomGossipProbe).probeCompleted(probeResult1);
        verify(gossips).probeInterval();
    }

    @Test
    void onPingBrokenDatagramPacket() {
        // given
        DatagramPacket datagramPacket = new DatagramPacket(Unpooled.copiedBuffer("broken".getBytes()), RECIPIENT, SENDER);
        BDDMockito.<Flux<?>>given(udpInbound.receiveObject()).willReturn(Flux.just(datagramPacket));

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError();
    }

    @Test
    void onPingDirect() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, ping))
                  .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                    .expectSubscription()
                    .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(RECIPIENT_NODE_ID)
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingError() {
        // given
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, ping)).willReturn(null);
        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError();
    }

    @Test
    void onPingErrorNodeIsNotADestination() {
        // given
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);

        gossipProtocol = new GossipProtocol(8888, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError();
    }

    @Test
    void onPingDirectDelayError() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.error(new RuntimeException()).cast(Long.class));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError();
    }

    @Test
    void onPingDirectDelay() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(RECIPIENT_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ofMillis(100)));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100))
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(RECIPIENT_NODE_ID)
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingIndirect() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(PROXY_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(false)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);

        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());
        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Flux.just(Ack.newBuilder().build()));

        gossipProtocol = new GossipProtocol(PROXY_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(PROXY_NODE_ID)
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingIndirectSendNackThenAck() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(PROXY_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(false)
                .setNackTimeout(10)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);

        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());
        // ACK will come later than NACK
        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Flux.just(Ack.newBuilder().build()).delayElements(Duration.ofMillis(100)));

        gossipProtocol = new GossipProtocol(PROXY_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAcks(
            Ack.newBuilder().setNodeId(PROXY_NODE_ID).setNack(true).addAllGossips(gossipList).build(),
            Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build()
        );
    }

    @Test
    void onPingIndirectSendAckBeforeNack() { // nack is ignored
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(PROXY_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(false)
                .setNackTimeout(100)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);

        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());
        // ACK will come earlier than NACK
        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Flux.just(Ack.newBuilder().build()).delayElements(Duration.ofMillis(10)));

        gossipProtocol = new GossipProtocol(PROXY_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAcks(
                Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build()
        );
    }

    @Test
    void onPingIndirectError() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(PROXY_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(false)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);

        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());

        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Flux.error(new RuntimeException("peer unavailable")));

        gossipProtocol = new GossipProtocol(PROXY_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(PROXY_NODE_ID)
                .setNack(true)
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingIndirectDelayError() {
        // given
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(PROXY_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(false)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);

        BDDMockito.given(udpOutbound.sendObject(any(DatagramPacket.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());

        BDDMockito.given(gossipTransport.ping(any(Ping.class)))
                .willReturn(Mono.delay(Duration.ofMillis(100)).thenMany(Flux.error(new RuntimeException("peer unavailable"))));

        gossipProtocol = new GossipProtocol(PROXY_NODE_ID, peers, gossips, randomGossipProbe, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(PROXY_NODE_ID)
                .setNack(true)
                .addAllGossips(gossipList)
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
    }

    private void assertAcks(Ack ...acks) {
        ArgumentCaptor<DatagramPacket> datagramPacketCapture = ArgumentCaptor.forClass(DatagramPacket.class);
        verify(udpOutbound, times(acks.length)).sendObject(datagramPacketCapture.capture());
        List<DatagramPacket> datagramPackets = datagramPacketCapture.getAllValues();
        for (int i=0; i < acks.length; i++) {
            DatagramPacket datagramPacket = datagramPackets.get(i);
            assertThat(datagramPacket.content()).isEqualTo(Unpooled.copiedBuffer(acks[i].toByteArray()));
        }
    }

    private void assertError() {
        verify(udpOutbound, never()).sendObject(any());
    }

}