package rsocket.playground.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerNodeOperations extends BaseNodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

    private static final Duration ELECTION_TIMEOUT = Duration.ofMillis(150);

    @Override
    public void onInit(Node node) {
        disposable = processor.timeout(ELECTION_TIMEOUT)
                .doOnError(throwable -> {
                    node.increaseCurrentTerm();
                    node.convertToCandidate();
                })
                .subscribe();
    }

    @Override
    public void onExit(Node node) {
        disposable.dispose();
    }

    @Override
    public Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries) {
        AppendEntriesResult appendEntriesResult = new AppendEntriesResult()
                .term(node.getCurrentTerm());

        return Mono.just(appendEntriesResult);
    }

    @Override
    public Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote) {
        RequestVoteResult requestVoteResult = new RequestVoteResult()
                .voteGranted(requestVote.getTerm() >= node.getCurrentTerm())
                .term(requestVote.getTerm());
        return Mono.just(requestVoteResult);
    }

}
