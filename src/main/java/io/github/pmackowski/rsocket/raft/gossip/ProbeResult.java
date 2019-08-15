package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;

import java.util.ArrayList;
import java.util.List;

public class ProbeResult {

    static final ProbeResult NO_PROBE_ACKS = new ProbeResult();

    private int destinationNodeId;
    private ProbeOperatorResult<Ack> probeOperatorResult;
    private int subgroupSize;
    private List<Gossip> hotGossips;

    public int getDestinationNodeId() {
        return destinationNodeId;
    }

    public List<Ack> getAcks() {
        return probeOperatorResult.getElements();
    }

    public int getSubgroupSize() {
        return subgroupSize;
    }

    public List<Gossip> getHotGossips() {
        return hotGossips;
    }

    public boolean hasAck() {
        return getAcks().stream().anyMatch(ack -> !ack.getNack());
    }

    private ProbeResult() {}

    public static ProbeResult.Builder builder() {
        return new ProbeResult.Builder();
    }

    public static class Builder {

        private int destinationNodeId;
        private ProbeOperatorResult<Ack> probeOperatorResult;
        private int subgroupSize;
        private List<Gossip> hotGossips = new ArrayList<>();

        private Builder() {
        }

        public ProbeResult.Builder destinationNodeId(int destinationNodeId) {
            this.destinationNodeId = destinationNodeId;
            return this;
        }

        public ProbeResult.Builder probeResult(ProbeOperatorResult<Ack> probeOperatorResult) {
            this.probeOperatorResult = probeOperatorResult;
            return this;
        }

        public ProbeResult.Builder subgroupSize(int subgroupSize) {
            this.subgroupSize = subgroupSize;
            return this;
        }

        public ProbeResult.Builder hotGossips(List<Gossip> hotGossips) {
            this.hotGossips = hotGossips;
            return this;
        }

        public ProbeResult build() {
            ProbeResult probeResult = new ProbeResult();
            probeResult.destinationNodeId = destinationNodeId;
            probeResult.probeOperatorResult = probeOperatorResult;
            probeResult.subgroupSize = subgroupSize;
            probeResult.hotGossips = hotGossips;
            return probeResult;
        }

    }
}