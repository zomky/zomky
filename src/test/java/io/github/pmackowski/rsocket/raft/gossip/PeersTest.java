package io.github.pmackowski.rsocket.raft.gossip;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PeersTest {

    @Test
    void nextRandomPeerId() {
        Peers peers = new Peers(new HashSet<>(Arrays.asList(1,2,3)));

        assertThat(Arrays.asList(peers.nextRandomPeerId(),peers.nextRandomPeerId(),peers.nextRandomPeerId())).containsExactlyInAnyOrder(1,2,3);
        assertThat(Arrays.asList(peers.nextRandomPeerId(),peers.nextRandomPeerId(),peers.nextRandomPeerId())).containsExactlyInAnyOrder(1,2,3);
    }

    @Test
    void selectCompanions() {
        Peers peers = new Peers(new HashSet<>(Arrays.asList(1,2,3)));

        List<Integer> actual = peers.selectCompanions(1, 20);

        assertThat(actual).containsExactlyInAnyOrder(2,3);
    }

    @Test
    void selectCompanions2() {
        Peers peers = new Peers(new HashSet<>(Arrays.asList(1,2,3)));

        List<Integer> actual = peers.selectCompanions(1, 1);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsAnyOf(2,3);
    }
}