package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;

public class FollowerNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

    private DirectProcessor<Payload> processor;
    private FluxSink<Payload> sink;
    private Disposable disposable;

    public FollowerNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public void onInit(Node node) {
        disposable = processor.timeout(ElectionTimeout.nextRandom())
                .onErrorResume(throwable -> {
                    LOGGER.info("node {}, {}", node.nodeId, throwable.getMessage());
                    node.convertToCandidate();
                    return Flux.empty();
                })
                .subscribe();
    }

    @Override
    public void onExit(Node node) {
        disposable.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .doOnNext(appendEntriesRequest -> {
                       if (appendEntriesRequest.getTerm() >= node.getCurrentTerm()) {
                           restartElectionTimer();
                       }
                   })
                   .map(appendEntries1 -> new AppendEntriesResponse()
                           .term(node.getCurrentTerm())
                           .success(appendEntries1  .getTerm() >= node.getCurrentTerm())
                   );
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       NodeData nodeData = node.getNodeData();

                       boolean voteGranted = requestVote.getTerm() >= nodeData.getCurrentTerm() &&
                                       nodeData.getVotedFor() == null;

                       if (voteGranted) {
                           node.voteFor(requestVote.getCandidateId());
                           restartElectionTimer();
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(nodeData.getCurrentTerm());
                   });
    }

    private void restartElectionTimer() {
        sink.next(DefaultPayload.create(""));
    }

}
