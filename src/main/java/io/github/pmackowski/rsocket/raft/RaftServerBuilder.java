package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import reactor.core.publisher.Mono;

import java.util.List;

public class RaftServerBuilder {

    private StateMachine stateMachine;
    private ElectionTimeout electionTimeout;
    private RaftStorage raftStorage;
    private int nodeId;
    private List<Integer> clientPorts;

    public RaftServerBuilder nodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public RaftServerBuilder clientPorts(List<Integer> clientPorts) {
        this.clientPorts = clientPorts;
        return this;
    }

    public RaftServerBuilder stateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        return this;
    }

    public RaftServerBuilder electionTimeout(ElectionTimeout electionTimeout) {
        this.electionTimeout = electionTimeout;
        return this;
    }

    public RaftServerBuilder storage(RaftStorage raftStorage) {
        this.raftStorage = raftStorage;
        return this;
    }

    public Mono<RaftServer> start() {
        return Mono.defer(() -> {
           DefaultRaftServer kvStoreServer = new DefaultRaftServer(nodeId, raftStorage, clientPorts, stateMachine, electionTimeout);
           return Mono.just(kvStoreServer).doOnNext(DefaultRaftServer::start);
        });
    }

}
