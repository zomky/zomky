package io.github.zomky.gossip;

import io.github.zomky.gossip.protobuf.Gossip;
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
    private Duration baseProbeTimeout;
    private float nackRatio;
    private float indirectDelayRatio;
    private GossipProbe gossipProbe;

    Mono<ProbeResult> randomProbe() {
        PeerProbe peerProbe = peers.nextPeerProbe(subgroupSize);
        PeerProbeTimeouts peerProbeTimeouts = PeerProbeTimeouts.builder()
                .baseProbeTimeout(baseProbeTimeout)
                .nackRatio(nackRatio)
                .indirectDelayRatio(indirectDelayRatio)
                .localHealthMultiplier(gossips.localHealthMultiplier())
                .build();
        if (PeerProbe.NO_PEER_PROBE.equals(peerProbe)) {
            LOGGER.trace("[Node {}][ping] No probing for one-node cluster", nodeId);
            return Mono.delay(peerProbeTimeouts.probeTimeout()).then(Mono.empty());
        }
        LOGGER.trace("[Node {}][ping] Probing {} ...", nodeId, peerProbe);
        List<Gossip> hotGossips = gossips.chooseHotGossips(peerProbe.getDestinationNodeId());
        return gossipProbe.probeNode(peerProbe, hotGossips, peerProbeTimeouts);
    }

    void probeCompleted(ProbeResult probeResult) {
        gossips.probeCompleted(probeResult);
        gossips.updateLocalHealthMultiplier(probeResult);
        if (probeResult.hasAck()) {
            LOGGER.trace("[Node {}][ping] Probing {} successful.", nodeId, probeResult.getDestinationNodeId());
        } else {
            LOGGER.trace("[Node {}][ping] Probing {} unsuccessful.", nodeId, probeResult.getDestinationNodeId());
        }
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
            randomGossipProbe.subgroupSize = subgroupSize;
            randomGossipProbe.nackRatio = nackRatio;
            randomGossipProbe.indirectDelayRatio = indirectDelayRatio;
            randomGossipProbe.gossipProbe = gossipProbe;
            return randomGossipProbe;
        }

    }
}
