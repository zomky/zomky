package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;

public class CandidateNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateNodeOperations.class);

    private static final int QUORUM = 2; // hardcoded (assuming cluster of 3 nodes)

    private Disposable disposable;

    @Override
    public void onInit(Node node) {
        ElectionContext electionContext = new ElectionContext(false);
        disposable = Mono.defer(() -> startElection(node, electionContext))
            .repeatWhen(leaderNotElected(electionContext))
            .subscribe();
    }

    @Override
    public void onExit(Node node) {
        disposable.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntries1 -> new AppendEntriesResponse().term(node.getCurrentTerm()));
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       boolean voteGranted = requestVote.getTerm() > node.getCurrentTerm();
                       if (voteGranted) {
                           node.convertToFollowerIfObsolete(requestVote.getTerm());
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(node.getCurrentTerm());
                   });
    }

    private Mono<Void> startElection(Node node, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(Node::increaseCurrentTerm)
                   .doOnNext(node1 -> node.voteFor(node.nodeId))
                   .then(sendVotes(node, electionContext));
    }

    private Mono<Void> sendVotes(Node node, ElectionContext electionContext) {
        VoteRequest requestVote = new VoteRequest()
                .term(node.getCurrentTerm())
                .candidateId(node.nodeId);

        return node.senders
                .flatMap(rSocket -> sendVoteRequest(rSocket, requestVote))
                //.doOnNext(voteResponse -> node.convertToFollowerIfObsolete(voteResponse.getTerm()))
                .filter(VoteResponse::isVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(QUORUM - 1)
                .timeout(ElectionTimeout.nextRandom())
                .onErrorResume(throwable -> {
                    electionContext.setRepeatElection(true);
                    LOGGER.info("Node {}, {}", node.nodeId, throwable.getMessage());
                    return Mono.empty();
                })
                .next()
                .doOnNext(s -> {
                    electionContext.setRepeatElection(false);
                    node.convertToLeader();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(RSocket rSocket, VoteRequest requestVote) {
        Payload payload = ObjectPayload.create(requestVote);
        return rSocket.requestResponse(payload)
                .map(payload1 -> ObjectPayload.dataFromPayload(payload1, VoteResponse.class))
                .onErrorResume(throwable -> {
                    LOGGER.error("--> {}", throwable.getMessage());
                    return Mono.just(VoteResponse.FALLBACK_RESPONSE).delayElement(Duration.ofSeconds(100));
                });
    }

    private Repeat<ElectionContext> leaderNotElected(ElectionContext electionContext) {
        return Repeat.<ElectionContext>onlyIf(repeatContext -> repeatContext.applicationContext().repeatElection())
                .withApplicationContext(electionContext);
    }

    private static class ElectionContext {

        private boolean repeatElection;

        ElectionContext(boolean repeatElection) {
            this.repeatElection = repeatElection;
        }

        public void setRepeatElection(boolean repeatElection) {
            this.repeatElection = repeatElection;
        }

        public boolean repeatElection() {
            return repeatElection;
        }
    }

}
