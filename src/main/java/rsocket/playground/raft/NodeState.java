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
    public void onInit(Node node) {
        this.nodeOperations.onInit(node);
    }

    @Override
    public void onExit(Node node) {
        this.nodeOperations.onExit(node);
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return this.nodeOperations.onAppendEntries(node, appendEntries);
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return this.nodeOperations.onRequestVote(node, requestVote);
    }

}
