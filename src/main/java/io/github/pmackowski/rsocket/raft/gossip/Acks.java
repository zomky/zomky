package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Acks {

    private Integer destinationNodeId = -1;
    private List<Ack> acks = new ArrayList<>();

    public Acks() {

    }

    public Acks(int destinationNodeId, Collection<Ack> acks) {
        this.destinationNodeId = destinationNodeId;
        this.acks.addAll(acks);
    }

    public Integer getDestinationNodeId() {
        return destinationNodeId;
    }

    public List<Ack> getAcks() {
        return acks;
    }

    @Override
    public String toString() {
        return "Acks{" +
                "destinationNodeId=" + destinationNodeId +
                ", acks=" + acks +
                '}';
    }
}
