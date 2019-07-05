package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import reactor.core.publisher.Mono;

public class RaftServerBuilder {

    private StateMachine stateMachine;
    private ElectionTimeout electionTimeout;
    private RaftStorage raftStorage;
    private int nodeId;
    private boolean preVote;
    private boolean leaderStickiness;
    private Configuration configuration = Configuration.DEFAULT_CONFIGURATION;
    private boolean passive;

    public RaftServerBuilder nodeId(int nodeId) {
        this.nodeId = nodeId;
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

    public RaftServerBuilder preVote(boolean preVote) {
        this.preVote = preVote;
        return this;
    }

    public RaftServerBuilder leaderStickiness(boolean leaderStickiness) {
        this.leaderStickiness = leaderStickiness;
        return this;
    }

    public RaftServerBuilder initialConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public RaftServerBuilder passive(boolean passive) {
        this.passive = passive;
        return this;
    }

    public Mono<RaftServer> start() {
        return Mono.defer(() -> {
           DefaultRaftServer kvStoreServer = new DefaultRaftServer(nodeId, raftStorage, configuration, stateMachine, electionTimeout, preVote, leaderStickiness, passive);
           return Mono.just(kvStoreServer).doOnNext(DefaultRaftServer::start);
        });
    }

}
