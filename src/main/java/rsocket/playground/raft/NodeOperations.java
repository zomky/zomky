package rsocket.playground.raft;

import reactor.core.publisher.Mono;
import rsocket.playground.raft.storage.ZomkyStorage;

public interface NodeOperations {

    void onInit(Node node, ZomkyStorage zomkyStorage);

    void onExit(Node node, ZomkyStorage zomkyStorage);

    Mono<AppendEntriesResponse> onAppendEntries(Node node, ZomkyStorage zomkyStorage, AppendEntriesRequest appendEntries);

    Mono<VoteResponse> onRequestVote(Node node, ZomkyStorage zomkyStorage, VoteRequest requestVote);

}
