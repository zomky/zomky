package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;

import java.util.*;
import java.util.stream.Collectors;

public class ProbeResult {

    private int destinationNodeId;
    private ProbeOperatorResult<Ack> probeOperatorResult;
    private int subgroupSize;
    private boolean missedNack;
    private List<Gossip> hotGossips;

    public int getDestinationNodeId() {
        return destinationNodeId;
    }

    public List<Ack> getAcks() {
        return probeOperatorResult.getElements();
    }

    public List<Ack> getDistinctAcks() {
        Map<Integer, Ack> ackByNodeId = probeOperatorResult.getElements().stream()
                .collect(Collectors.groupingBy(Ack::getNodeId, Collectors.collectingAndThen(Collectors.toList(),
                        acks -> acks.stream().filter(ack -> !ack.getNack()).findFirst().orElse(acks.get(0))))
                );
        return new ArrayList<>(ackByNodeId.values());
    }

    public int getSubgroupSize() {
        return subgroupSize;
    }

    List<Gossip> getHotGossips() {
        return hotGossips;
    }

    boolean hasAck() {
        return getAcks().stream().anyMatch(ack -> !ack.getNack());
    }

    boolean hasMissedNack() {
        return missedNack;
    }

    private ProbeResult() {}

    public static ProbeResult.Builder builder() {
        return new ProbeResult.Builder();
    }

    public static class Builder {

        private int destinationNodeId;
        private ProbeOperatorResult<Ack> probeOperatorResult;
        private int subgroupSize;
        private boolean missedNack;
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

        public ProbeResult.Builder missedNack(boolean missedNack) {
            this.missedNack = missedNack;
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
            probeResult.missedNack = missedNack;
            probeResult.hotGossips = hotGossips;
            return probeResult;
        }

    }
}