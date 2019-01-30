package rsocket.playground.raft;

import io.rsocket.Payload;
import reactor.core.publisher.Mono;

public interface NodeOperations {

    void onInit(Node node);

    void onExit(Node node);

    Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries);

    Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote);

    Mono<Payload> onPayloadRequest(Node node, Payload payload);
}
