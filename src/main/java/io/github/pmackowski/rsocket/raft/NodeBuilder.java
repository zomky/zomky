package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class NodeBuilder {

    private int nodeId;
    private RaftConfiguration raftConfiguration;
    private RaftStorageConfiguration raftStorageConfiguration;
    private Cluster cluster;

    public NodeBuilder nodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public NodeBuilder raftConfiguration(RaftConfiguration raftConfiguration) {
        this.raftConfiguration = raftConfiguration;
        return this;
    }

    public NodeBuilder raftConfiguration(RaftConfiguration.Builder defaultBuilder, Function<RaftConfiguration.Builder,RaftConfiguration.Builder> builderFunction) {
        this.raftConfiguration = builderFunction.apply(defaultBuilder).build();
        return this;
    }

    public NodeBuilder raftStorageConfiguration(RaftStorageConfiguration raftStorageConfiguration) {
        this.raftStorageConfiguration = raftStorageConfiguration;
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
