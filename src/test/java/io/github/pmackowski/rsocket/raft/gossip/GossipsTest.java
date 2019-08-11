package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import org.junit.jupiter.api.Test;

class GossipsTest {

    @Test
    void name() {
        Gossips gossips = new Gossips(7000);

//        gossips.addGossip(7000, Gossip.Suspicion.SUSPECT);
//        gossips.addGossip(7000, 0, Gossip.Suspicion.SUSPECT);
    }
}