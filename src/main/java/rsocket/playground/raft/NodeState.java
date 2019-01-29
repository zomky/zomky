package rsocket.playground.raft;

import reactor.core.publisher.Mono;

public enum NodeState implements NodeOperations {

    FOLLOWER(new FollowerNodeOperations()),

    CANDIDATE(new CandidateNodeOperations()),

    LEADER(new LeaderNodeOperations());

    private NodeOperations nodeOperations;

    NodeState(NodeOperations nodeOperations) {
        this.nodeOperations = nodeOperations;
    }

    @Override
    public Mono<Void> onInit(Node node) {
        return this.nodeOperations.onInit(node);
    }

    @Override
    public Mono<Void> onExit(Node node) {
        return this.nodeOperations.onExit(node);
    }


}
