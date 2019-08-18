package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

class RandomGossipProbe {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomGossipProbe.class);

    private int nodeId;
    private Peers peers;
    private Gossips gossips;
    private int subgroupSize;
    private Duration baseProbeInterval;
    private Duration baseProbeTimeout;
    private float nackRatio;
    private float indirectDelayRatio;
    private GossipProbe gossipProbe;

    Mono<ProbeResult> randomProbe() {
        PeerProbe peerProbe = peers.nextPeerProbe(subgroupSize);
        if (peerProbe.getDestinationNodeId() == null) {
            LOGGER.info("[Node {}][ping] No probing for one-node cluster", nodeId);
        } else {
            LOGGER.info("[Node {}][ping] Probing {} ...", nodeId, peerProbe);
        }
        PeerProbeTimeouts peerProbeTimeouts = PeerProbeTimeouts.builder()
                .baseProbeTimeout(baseProbeTimeout)
                .nackRatio(nackRatio)
                .indirectDelayRatio(indirectDelayRatio)
                .localHealthMultiplier(gossips.localHealthMultiplier())
                .build();
        List<Gossip> hotGossips = gossips.chooseHotGossips(peers.count());
        return gossipProbe.probeNode(peerProbe, hotGossips, peerProbeTimeouts);
    }

    void probeCompleted(ProbeResult probeResult) {
        gossips.probeCompleted(probeResult);
        gossips.updateLocalHealthMultiplier(probeResult);
        LOGGER.info("[Node {}][ping] Probing {} finished.", nodeId, probeResult.getDestinationNodeId());
    }

    Duration probeInterval() {
        return Duration.ofMillis(baseProbeInterval.toMillis() * (gossips.localHealthMultiplier() + 1));
    }

    private RandomGossipProbe() {}

    public static RandomGossipProbe.Builder builder() {
        return new RandomGossipProbe.Builder();
    }

    public static class Builder {

        private int nodeId;
        private Peers peers;
        private Gossips gossips;
        private Duration baseProbeTimeout;
        private Duration baseProbeInterval;
        private int subgroupSize;
        private float nackRatio;
        private float indirectDelayRatio;
        private GossipProbe gossipProbe;

        private Builder() {
        }

        public RandomGossipProbe.Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public RandomGossipProbe.Builder peers(Peers peers) {
            this.peers = peers;
            return this;
        }

        public RandomGossipProbe.Builder gossips(Gossips gossips) {
            this.gossips = gossips;
            return this;
        }

        public RandomGossipProbe.Builder baseProbeTimeout(Duration baseProbeTimeout) {
            this.baseProbeTimeout = baseProbeTimeout;
            return this;
        }

        public RandomGossipProbe.Builder baseProbeInterval(Duration baseProbeInterval) {
            this.baseProbeInterval = baseProbeInterval;
            return this;
        }

        public RandomGossipProbe.Builder subgroupSize(int subgroupSize) {
            this.subgroupSize = subgroupSize;
            return this;
        }

        public RandomGossipProbe.Builder nackRatio(float nackRatio) {
            this.nackRatio = nackRatio;
            return this;
        }

        public RandomGossipProbe.Builder indirectDelayRatio(float indirectDelayRatio) {
            this.indirectDelayRatio = indirectDelayRatio;
            return this;
        }

        public RandomGossipProbe.Builder gossipProbe(GossipProbe gossipProbe) {
            this.gossipProbe = gossipProbe;
            return this;
        }

        public RandomGossipProbe build() {
            RandomGossipProbe randomGossipProbe = new RandomGossipProbe();
            randomGossipProbe.nodeId = nodeId;
            randomGossipProbe.peers = peers;
            randomGossipProbe.gossips = gossips;
            randomGossipProbe.baseProbeTimeout = baseProbeTimeout;
            randomGossipProbe.baseProbeInterval = baseProbeInterval;
            randomGossipProbe.subgroupSize = subgroupSize;
            randomGossipProbe.nackRatio = nackRatio;
            randomGossipProbe.indirectDelayRatio = indirectDelayRatio;
            randomGossipProbe.gossipProbe = gossipProbe;
            return randomGossipProbe;
        }

    }
}
