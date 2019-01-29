package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;

public class CandidateNodeOperations implements NodeOperations{

    @Override
    public Mono<Void> onInit(Node node) {
        return null;
    }

    @Override
    public Mono<Void> onExit(Node node) {
        return null;
    }

    private Mono<Long> sendVotes(Node node) {
        RequestVote requestVote = new RequestVote().term(1).candidateId(1);

        return node.otherServers
                .flatMap(rSocket -> sendVoteRequest(rSocket, requestVote))
                .buffer()
                .map(list -> list.stream()
                        .map(RequestVoteResult::isVoteGranted)
                        .filter(Boolean::booleanValue)
                        .count() + 1
                ).next();
    }

    private Mono<RequestVoteResult> sendVoteRequest(RSocket rSocket, RequestVote requestVote) {
        Payload payload = ObjectPayload.create(requestVote, Command.REQUEST_VOTE.getCommandNo());
        return rSocket.requestResponse(payload)
                .timeout(Duration.ofMillis(100))
                .map(payload1 -> ObjectPayload.dataFromPayload(payload1, RequestVoteResult.class))
                .onErrorResume(throwable -> Mono.just(new RequestVoteResult().voteGranted(true)));
    }

}
