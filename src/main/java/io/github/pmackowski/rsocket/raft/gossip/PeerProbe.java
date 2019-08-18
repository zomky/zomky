package io.github.pmackowski.rsocket.raft.gossip;

import java.util.ArrayList;
import java.util.List;

class PeerProbe {

    static final PeerProbe NO_PEER_PROBE = new PeerProbe();

    private Integer destinationNodeId;
    private List<Integer> proxyNodeIds;

    private PeerProbe() {
    }

    PeerProbe(Integer destinationNodeId) {
        this(destinationNodeId, new ArrayList<>());
    }

    PeerProbe(Integer destinationNodeId, List<Integer> proxyNodeIds) {
        this.destinationNodeId = destinationNodeId;
        this.proxyNodeIds = proxyNodeIds;
    }

    public Integer getDestinationNodeId() {
        return destinationNodeId;
    }

    public List<Integer> getProxyNodeIds() {
        return proxyNodeIds;
    }

    public int getSubgroupSize() {
        return proxyNodeIds.size();
    }

    @Override
    public String toString() {
        return "[destinationNodeId=" + destinationNodeId + ", proxyNodeIds=" + proxyNodeIds + ']';
    }
}
