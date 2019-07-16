package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import reactor.core.publisher.Mono;

public class NodeBuilder {

    private int nodeId;
    private Cluster cluster;

    public NodeBuilder nodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public NodeBuilder cluster(Cluster cluster) {
        this.cluster = cluster;
        return this;
    }

    public Mono<Node> start() {
        return Mono.defer(() -> {
           DefaultNode kvStoreServer = new DefaultNode(nodeId, cluster);
           return Mono.just(kvStoreServer).doOnNext(DefaultNode::start);
        });
    }

}
