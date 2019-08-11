package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ProbeAcks {

    static final ProbeAcks NO_PROBE_ACKS = new ProbeAcks();

    private int destinationNodeId;
    private List<Ack> acks = new ArrayList<>();
    private int maxAcks;

    public int getDestinationNodeId() {
        return destinationNodeId;
    }

    public List<Ack> getAcks() {
        return acks;
    }

    public int getMaxAcks() {
        return maxAcks;
    }

    @Override
    public String toString() {
        return "Acks{" +
                "destinationNodeId=" + destinationNodeId +
                ", acks=" + acks +
                '}';
    }

    private ProbeAcks() {}

    public static ProbeAcks.Builder builder() {
        return new ProbeAcks.Builder();
    }

    public static class Builder {

        private int destinationNodeId;
        private List<Ack> acks = new ArrayList<>();
        private int maxAcks;

        private Builder() {
        }

        public ProbeAcks.Builder destinationNodeId(int destinationNodeId) {
            this.destinationNodeId = destinationNodeId;
            return this;
        }

        public ProbeAcks.Builder acks(Collection<Ack> acks) {
            this.acks.addAll(acks);
            return this;
        }

        public ProbeAcks.Builder maxAcks(int maxAcks) {
            this.maxAcks = maxAcks;
            return this;
        }

        public ProbeAcks build() {
            ProbeAcks probeAcks = new ProbeAcks();
            probeAcks.destinationNodeId = destinationNodeId;
            probeAcks.acks = acks;
            probeAcks.maxAcks = maxAcks;
            return probeAcks;
        }

    }
}