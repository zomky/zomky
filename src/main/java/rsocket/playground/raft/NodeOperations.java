package rsocket.playground.raft;

import reactor.core.publisher.Mono;

public interface NodeOperations {

    void onInit(Node node);

    void onExit(Node node);

    Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries);

    Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote);

}
