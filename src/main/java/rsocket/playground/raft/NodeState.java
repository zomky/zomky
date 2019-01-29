package rsocket.playground.raft;

import io.rsocket.Payload;
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
    public Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries) {
        return this.nodeOperations.onAppendEntries(node, appendEntries);
    }

    @Override
    public Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote) {
        return this.nodeOperations.onRequestVote(node, requestVote);
    }

    @Override
    public Mono<Payload> onPayloadRequest(Payload payload) {
        return this.nodeOperations.onPayloadRequest(payload);
    }

}
