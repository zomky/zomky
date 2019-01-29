package rsocket.playground.raft;

import io.rsocket.Payload;
import reactor.core.publisher.Mono;

public interface NodeOperations {

    void onInit(Node node);

    void onExit(Node node);

    Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries);

    Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote);

    Mono<Payload> onPayloadRequest(Payload payload);
}
