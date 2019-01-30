package rsocket.playground.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;

public class FollowerNodeOperations extends BaseNodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

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
                   .map(appendEntries1 -> new AppendEntriesResponse()
                           .term(node.getCurrentTerm())
                   );
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       NodeData nodeData = node.getNodeData();

                       boolean voteGranted =
                               requestVote.getTerm() >= nodeData.getCurrentTerm() &&
                                       nodeData.getVotedFor() == null;

                       if (voteGranted) {
                           H2.updateVotedFor(nodeData.getNodeId(), requestVote.getCandidateId());
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(requestVote.getTerm());
                   });
    }

}
