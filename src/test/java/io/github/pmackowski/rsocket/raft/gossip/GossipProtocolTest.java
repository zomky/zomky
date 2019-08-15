package io.github.pmackowski.rsocket.raft.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
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
import static org.mockito.Mockito.verify;

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
    void onPingBrokenDatagramPacket() {
        // given
        DatagramPacket datagramPacket = new DatagramPacket(Unpooled.copiedBuffer("broken".getBytes()), RECIPIENT, SENDER);
        BDDMockito.<Flux<?>>given(udpInbound.receiveObject()).willReturn(Flux.just(datagramPacket));
        BDDMockito.given(udpOutbound.sendObject(any(Mono.class))).willReturn(nettyOutbound);

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError(GossipException.class, InvalidProtocolBufferException.class);
    }

    @Test
    void onPingDirect() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(RECIPIENT_NODE_ID);
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
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, NUMBER_OF_PEERS, ping))
                  .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                    .expectSubscription()
                    .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(node.getNodeId())
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingError() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(RECIPIENT_NODE_ID);

        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(Mono.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, NUMBER_OF_PEERS, ping)).willReturn(null);
        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError(GossipException.class, "onPing unexpected error");
    }

    @Test
    void onPingErrorInitiatorAndRequestorAreDifferent() {
        // given
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(8888)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(Mono.class))).willReturn(nettyOutbound);

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError(GossipException.class, "Initiator and requestor node must be the same! [7000,8888]");
    }

    @Test
    void onPingErrorNodeIsNotADestination() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(8888);

        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(new ArrayList<>())
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(Mono.class))).willReturn(nettyOutbound);

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError(GossipException.class, "This node is not a destination node! [7001,8888]");
    }

    @Test
    void onPingDirectDelayError() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(RECIPIENT_NODE_ID);
        List<Gossip> gossipList = new ArrayList<>();
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(SENDER_NODE_ID)
                .setRequestorNodeId(SENDER_NODE_ID)
                .setDestinationNodeId(RECIPIENT_NODE_ID)
                .setDirect(true)
                .addAllGossips(gossipList)
                .build();
        givenPing(ping);
        BDDMockito.given(udpOutbound.sendObject(any(Mono.class))).willReturn(nettyOutbound);
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, NUMBER_OF_PEERS, ping))
                .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.error(new RuntimeException()).cast(Long.class));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertError(GossipException.class, "onPing direct ping error");
    }

    @Test
    void onPingDirectDelay() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(RECIPIENT_NODE_ID);
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
        BDDMockito.given(gossips.onPing(RECIPIENT_NODE_ID, NUMBER_OF_PEERS, ping))
                .willReturn(Ack.newBuilder().setNodeId(RECIPIENT_NODE_ID).addAllGossips(gossipList).build());

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ofMillis(100)));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100))
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(node.getNodeId())
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingIndirect() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(PROXY_NODE_ID);
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
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, NUMBER_OF_PEERS, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());
        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Mono.just(Ack.newBuilder().build()));

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(node.getNodeId())
                .addAllGossips(gossipList)
                .build()
        );
    }

    @Test
    void onPingIndirectError() {
        // given
        BDDMockito.given(node.getNodeId()).willReturn(PROXY_NODE_ID);
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
        BDDMockito.given(gossips.onPing(PROXY_NODE_ID, NUMBER_OF_PEERS, ping))
                .willReturn(Ack.newBuilder().setNodeId(PROXY_NODE_ID).addAllGossips(gossipList).build());

        BDDMockito.given(gossipTransport.ping(any(Ping.class))).willReturn(Mono.error(new RuntimeException("peer unavailable")));

        gossipProtocol = new GossipProtocol(node, peers, gossips, gossipTransport, (nodeId, counter) -> Mono.delay(Duration.ZERO));

        // then
        StepVerifier.create(gossipProtocol.onPing(udpInbound, udpOutbound))
                .expectSubscription()
                .verifyComplete();

        assertAck(Ack.newBuilder()
                .setNodeId(node.getNodeId())
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

    private void assertError(Class<?> exceptionType, String expectedErrorMessage) {
        ArgumentCaptor<Mono> objectCapture = ArgumentCaptor.forClass(Mono.class);
        verify(udpOutbound).sendObject(objectCapture.capture());
        Object obj = objectCapture.getValue();
        assertThat(obj).isInstanceOf(Publisher.class);
        StepVerifier.create((Publisher) obj)
                .expectSubscription()
                .consumeErrorWith(exception -> {
                    assertThat(exception).isInstanceOf(exceptionType);
                    assertThat(exception.getMessage()).isEqualTo(expectedErrorMessage);
                })
                .verify();
    }

    private void assertError(Class<?> exceptionType, Class<?> causeType) {
        ArgumentCaptor<Mono> objectCapture = ArgumentCaptor.forClass(Mono.class);
        verify(udpOutbound).sendObject(objectCapture.capture());
        Object obj = objectCapture.getValue();
        assertThat(obj).isInstanceOf(Publisher.class);
        StepVerifier.create((Publisher) obj)
                .expectSubscription()
                .consumeErrorWith(exception -> {
                    assertThat(exception).isInstanceOf(exceptionType);
                    assertThat(exception.getCause()).isInstanceOf(causeType);
                })
                .verify();
    }
}