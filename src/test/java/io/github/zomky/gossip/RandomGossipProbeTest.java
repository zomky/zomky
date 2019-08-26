package io.github.zomky.gossip;

import io.github.zomky.gossip.protobuf.Gossip;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RandomGossipProbeTest {

    private static final int INITIATOR_NODE_ID = 7000;
    private static final int DESTINATION_NODE_ID = 7001;
    private static final Duration BASE_PROBE_TIMEOUT = Duration.ofMillis(500);

    private static final int SUBGROUP_SIZE = 2;
    private static final float INDIRECT_DELAY_RATIO = 0.3f;
    private static final float NACK_RATIO = 0.6f;
    private static final int LOCAL_HEALTH_MULTIPLIER = 3;

    @Mock Peers peers;
    @Mock Gossips gossips;
    @Mock GossipProbe gossipProbe;
    @Mock ProbeResult probeResult;
    PeerProbe peerProbe;
    List<Gossip> hotGossips;
    RandomGossipProbe randomGossipProbe;


    @BeforeEach
    void setUp() {
        peerProbe = new PeerProbe(DESTINATION_NODE_ID);
        hotGossips = Collections.singletonList(Gossip.newBuilder().build());
        randomGossipProbe = RandomGossipProbe.builder()
                .peers(peers)
                .gossips(gossips)
                .gossipProbe(gossipProbe)
                .nackRatio(NACK_RATIO)
                .indirectDelayRatio(INDIRECT_DELAY_RATIO)
                .subgroupSize(SUBGROUP_SIZE)
                .baseProbeTimeout(BASE_PROBE_TIMEOUT)
                .nodeId(INITIATOR_NODE_ID)
                .build();

    }

    @Test
    void randomProbeOneNodeCluster() {
        // given
        given(peers.nextPeerProbe(SUBGROUP_SIZE)).willReturn(PeerProbe.NO_PEER_PROBE);
        given(gossips.localHealthMultiplier()).willReturn(LOCAL_HEALTH_MULTIPLIER);

        // when
        StepVerifier.create(randomGossipProbe.randomProbe())
                .expectSubscription()
                .verifyComplete();

        verify(gossips, never()).chooseHotGossips();
        verify(gossipProbe, never()).probeNode(any(), any(), any());
    }

    @Test
    void randomProbe() {
        // given
        given(peers.nextPeerProbe(SUBGROUP_SIZE)).willReturn(peerProbe);
        given(gossips.localHealthMultiplier()).willReturn(LOCAL_HEALTH_MULTIPLIER);
        given(gossips.chooseHotGossips()).willReturn(hotGossips);
        ArgumentCaptor<PeerProbeTimeouts> argumentCaptor = ArgumentCaptor.forClass(PeerProbeTimeouts.class);
        given(gossipProbe.probeNode(eq(peerProbe), eq(hotGossips), argumentCaptor.capture()))
                .willReturn(Mono.just(probeResult));

        // when
        StepVerifier.withVirtualTime(() -> randomGossipProbe.randomProbe())
                .expectNext(probeResult)
                .verifyComplete();

        // then
        PeerProbeTimeouts peerProbeTimeouts = argumentCaptor.getValue();
        assertThat(peerProbeTimeouts.probeTimeout()).isEqualTo(Duration.ofMillis(2000));
        assertThat(peerProbeTimeouts.indirectDelay()).isEqualTo(Duration.ofMillis(600));
        assertThat(peerProbeTimeouts.nackTimeout()).isEqualTo(Duration.ofMillis(840));
    }

    @Test
    void probeCompleted() {
        // when
        given(probeResult.getDestinationNodeId()).willReturn(DESTINATION_NODE_ID);
        randomGossipProbe.probeCompleted(probeResult);

        // then
        verify(gossips).probeCompleted(probeResult);
        verify(gossips).updateLocalHealthMultiplier(probeResult);
    }

}