package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class FollowerNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

    private DirectProcessor<Payload> processor;
    private FluxSink<Payload> sink;
    private Disposable subscription;

    public FollowerNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public void onInit(Node node) {
        subscription = processor.timeout(ElectionTimeout.nextRandom())
                .subscribe(payload -> {}, throwable -> {
                    LOGGER.info("[Node {}] Election timeout ({})", node.nodeId, throwable.getMessage());
                    node.convertToCandidate();
                });
    }

    @Override
    public void onExit(Node node) {
        subscription.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntriesRequest -> {
                       long currentTerm = node.getCurrentTerm();
                       if (appendEntriesRequest.getTerm() >= currentTerm) {
                           restartElectionTimer(node);
                       }
                       if (appendEntriesRequest.getTerm() > currentTerm) {
                           node.setCurrentTerm(appendEntriesRequest.getTerm());
                       }
                       return new AppendEntriesResponse()
                               .term(currentTerm)
                               .success(true); // TODO
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       long currentTerm = node.getCurrentTerm();

                       if (requestVote.getTerm() < currentTerm) {
                           return new VoteResponse().term(currentTerm).voteGranted(false);
                       }

                       boolean voteGranted = node.notVoted(requestVote.getTerm());

                       if (voteGranted) {
                           node.voteForCandidate(requestVote.getCandidateId(), requestVote.getTerm());
                           restartElectionTimer(node);
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(currentTerm);
                   });
    }

    private void restartElectionTimer(Node node) {
        LOGGER.debug("[Node {}] restartElectionTimer ...", node.nodeId);
        sink.next(DefaultPayload.create(""));
    }

}
