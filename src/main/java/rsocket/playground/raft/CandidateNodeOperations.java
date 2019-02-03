package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.transport.ObjectPayload;

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
                   .map(appendEntries1 -> {
                       long currentTerm = node.getCurrentTerm();
                       if (appendEntries1.getTerm() >= currentTerm) {
                           node.convertToFollower(appendEntries1.getTerm());
                       }
                       return new AppendEntriesResponse().term(currentTerm);
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       long currentTerm = node.getCurrentTerm();
                       boolean voteGranted = node.notVoted(requestVote.getTerm());
                       if (voteGranted) {
                           node.convertToFollower(requestVote.getTerm());
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(currentTerm);
                   });
    }

    private Mono<Void> startElection(Node node, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(Node::voteForMyself)
                   .then(sendVotes(node, electionContext));
    }

    private Mono<Void> sendVotes(Node node, ElectionContext electionContext) {
        VoteRequest requestVote = new VoteRequest()
                .term(node.getCurrentTerm())
                .candidateId(node.nodeId);

        return node.availableSenders()
                .flatMap(sender -> sendVoteRequest(sender, requestVote))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > node.getCurrentTerm()) {
                        node.convertToFollower(voteResponse.getTerm());
                    }
                })
                .filter(VoteResponse::isVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(QUORUM - 1)
                .timeout(ElectionTimeout.nextRandom())
                .onErrorResume(throwable -> {
                    electionContext.setRepeatElection(true);
                    LOGGER.info("Node {}. Repeat election due to timeout {}", node.nodeId, throwable.getMessage());
                    return Mono.empty();
                })
                .next()
                .doOnNext(s -> {
                    electionContext.setRepeatElection(false);
                    node.convertToLeader();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(Sender sender, VoteRequest requestVote) {
        Payload payload = ObjectPayload.create(requestVote);
        return sender.getRSocket().requestResponse(payload)
                .map(payload1 -> ObjectPayload.dataFromPayload(payload1, VoteResponse.class))
                .onErrorResume(throwable -> {
                    LOGGER.error("Node {}", requestVote.getCandidateId(), throwable);
                    return Mono.just(VoteResponse.FALLBACK_RESPONSE);
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
