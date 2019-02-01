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
                .subscribe(payload -> {}, throwable -> {
                    LOGGER.info("node {} - {}", node.nodeId, throwable.getMessage());
                    node.convertToCandidate();
                });
    }

    @Override
    public void onExit(Node node) {
        disposable.dispose();
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
                       NodeData nodeData = node.getNodeData();
                       long currentTerm = nodeData.getCurrentTerm();
                       Integer votedFor = nodeData.getVotedFor();
                       boolean voteGranted = requestVote.getTerm() > currentTerm ||
                               (requestVote.getTerm() == currentTerm && votedFor == null);

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
        LOGGER.info("Node {} restartElectionTimer ...", node);
        sink.next(DefaultPayload.create(""));
    }

}
