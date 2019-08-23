package io.github.zomky.gossip;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class PeersTest {

    @Test
    void peersAreEmpty() {
        Peers peers = new Peers(7001);

        assertThat(peers.nextPeerProbe(1)).isEqualTo(PeerProbe.NO_PEER_PROBE);
    }

    @Test
    void onePeer() {
        Peers peers = new Peers(7000);
        peers.add(7001);

        PeerProbe actual = peers.nextPeerProbe(1);

        assertThat(actual.getDestinationNodeId()).isEqualTo(7001);
        assertThat(actual.getProxyNodeIds()).isEmpty();
    }

    @Test
    void twoPeers() {
        Peers peers = new Peers(7000);
        peers.add(7001);
        peers.add(7002);

        PeerProbe actual = peers.nextPeerProbe(1);

        assertThat(actual.getDestinationNodeId()).isIn(7001,7002);
        assertThat(actual.getProxyNodeIds()).hasSize(1).containsAnyOf(7001,7002);
        assertThat(actual.getProxyNodeIds()).doesNotContain(actual.getDestinationNodeId());
    }

    @Test
    void threePeers() {
        Peers peers = new Peers(7000);
        peers.add(7001);
        peers.add(7002);
        peers.add(7003);

        PeerProbe actual = peers.nextPeerProbe(1);

        assertThat(actual.getDestinationNodeId()).isIn(7001,7002,7003);
        assertThat(actual.getProxyNodeIds()).hasSize(1).containsAnyOf(7001,7002,7003);
        assertThat(actual.getProxyNodeIds()).hasSize(1).doesNotContain(actual.getDestinationNodeId());
    }

    @Test
    void manyPeersRoundRobinSelection() {
        Peers peers = new Peers(7000);
        peers.add(7001);
        peers.add(7002);
        peers.add(7003);

        PeerProbe actual1 = peers.nextPeerProbe(1);
        PeerProbe actual2 = peers.nextPeerProbe(2);
        PeerProbe actual3 = peers.nextPeerProbe(2);
        PeerProbe actual4 = peers.nextPeerProbe(2);
        PeerProbe actual5 = peers.nextPeerProbe(2);
        PeerProbe actual6 = peers.nextPeerProbe(2);

        assertThat(Arrays.asList(actual1.getDestinationNodeId(), actual2.getDestinationNodeId(), actual3.getDestinationNodeId()))
                .containsExactlyInAnyOrder(7001, 7002, 7003);
        assertThat(Arrays.asList(actual4.getDestinationNodeId(), actual5.getDestinationNodeId(), actual6.getDestinationNodeId()))
                .containsExactlyInAnyOrder(7001, 7002, 7003);

        assertThat(actual1.getProxyNodeIds())
                .hasSize(1)
                .doesNotContain(actual1.getDestinationNodeId());
        assertThat(actual2.getProxyNodeIds())
                .hasSize(2)
                .doesNotContain(actual2.getDestinationNodeId());
    }
}