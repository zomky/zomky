package io.github.pmackowski.rsocket.raft.gossip;

import java.util.List;

public class PeerProbe {

    static final PeerProbe NO_PEER_PROBE = new PeerProbe();

    private Integer destinationNodeId;
    private List<Integer> proxyNodeIds;

    private PeerProbe() {
    }

    public PeerProbe(Integer destinationNodeId, List<Integer> proxyNodeIds) {
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
}
