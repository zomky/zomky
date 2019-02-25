package rsocket.playground.raft;

import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.storage.ZomkyStorage;

public enum NodeState implements NodeOperations {

    FOLLOWER(new FollowerNodeOperations()),

    CANDIDATE(new CandidateNodeOperations()),

    LEADER(new LeaderNodeOperations());

    private NodeOperations nodeOperations;

    NodeState(NodeOperations nodeOperations) {
        this.nodeOperations = nodeOperations;
    }

    @Override
    public void onInit(Node node, ZomkyStorage zomkyStorage) {
        this.nodeOperations.onInit(node, zomkyStorage);
    }

    @Override
    public void onExit(Node node, ZomkyStorage zomkyStorage) {
        this.nodeOperations.onExit(node, zomkyStorage);
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, ZomkyStorage zomkyStorage, AppendEntriesRequest appendEntries) {
        return this.nodeOperations.onAppendEntries(node, zomkyStorage, appendEntries);
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, ZomkyStorage zomkyStorage, VoteRequest requestVote) {
        return this.nodeOperations.onRequestVote(node, zomkyStorage, requestVote);
    }

    @Override
    public Mono<Payload> onClientRequest(Node node, ZomkyStorage zomkyStorage, Payload payload) {
        return this.nodeOperations.onClientRequest(node, zomkyStorage, payload);
    }

    @Override
    public Flux<Payload> onClientRequests(Node node, ZomkyStorage zomkyStorage, Publisher<Payload> payloads) {
        return this.nodeOperations.onClientRequests(node, zomkyStorage, payloads);
    }

}
