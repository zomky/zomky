package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;

public class CandidateNodeOperations extends BaseNodeOperations {

    private static final Duration ELECTION_TIMEOUT = Duration.ofMillis(150);

    @Override
    public void onInit(Node node) {
        long votes = sendVotes(node);
        // TODO in loop
        if (votes >= 1) { // TODO majority
            node.convertToLeader();
        }
    }

    @Override
    public void onExit(Node node) {

    }

    @Override
    public Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries) {
        AppendEntriesResult appendEntriesResult = new AppendEntriesResult()
                .term(node.getCurrentTerm());

        return Mono.just(appendEntriesResult).doOnNext(r -> node.convertToFollower());
    }

    @Override
    public Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote) {
        if (node.isGreaterThanCurrentTerm(requestVote.getTerm())) {

        }
        return null;
    }

    private long sendVotes(Node node) {
        RequestVote requestVote = new RequestVote()
                .term(node.getCurrentTerm())
                .candidateId(node.nodeId);

        return node.senders
                .publishOn(Schedulers.newElastic("vote-request"))
                .flatMap(rSocket -> sendVoteRequest(rSocket, requestVote))
                .buffer()
                .map(list -> list.stream()
                        .map(RequestVoteResult::isVoteGranted)
                        .filter(Boolean::booleanValue)
                        .count() + 1
                )
                // if RequestVoteResult#term > current term T, set currentTerm = T and convert to follower
                .blockLast(ELECTION_TIMEOUT);
    }

    private Mono<RequestVoteResult> sendVoteRequest(RSocket rSocket, RequestVote requestVote) {
        Payload payload = ObjectPayload.create(requestVote);
        return rSocket.requestResponse(payload)
                .timeout(Duration.ofMillis(100))
                .map(payload1 -> ObjectPayload.dataFromPayload(payload1, RequestVoteResult.class))
                .onErrorResume(throwable -> Mono.just(new RequestVoteResult().voteGranted(true)));
    }

}
