package rsocket.playground.raft;

import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;
import rsocket.playground.raft.transport.ObjectPayload;

public class CandidateNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateNodeOperations.class);

    private static final int QUORUM = 2; // hardcoded (assuming cluster of 3 nodes)

    private Disposable subscription;

    @Override
    public Mono<Payload> onClientRequest(Node node, ZomkyStorage zomkyStorage, Payload payload) {
        return Mono.error(new RaftException("I am not a leader!"));
    }

    @Override
    public void onInit(Node node, ZomkyStorage zomkyStorage) {
        ElectionContext electionContext = new ElectionContext(false);
        subscription = Mono.defer(() -> startElection(node, zomkyStorage, electionContext))
                .repeatWhen(leaderNotElected(electionContext))
                .subscribe();
    }

    @Override
    public void onExit(Node node, ZomkyStorage zomkyStorage) {
        subscription.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, ZomkyStorage zomkyStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntries1 -> {
                       int currentTerm = zomkyStorage.getTerm();
                       if (appendEntries1.getTerm() >= currentTerm) {
                           node.convertToFollower(appendEntries1.getTerm());
                       }
                       return new AppendEntriesResponse().term(currentTerm).success(true);
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, ZomkyStorage zomkyStorage, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       boolean voteGranted = node.notVoted(requestVote.getTerm());
                       if (voteGranted) {
                           node.convertToFollower(requestVote.getTerm());
                       }
                       return new VoteResponse()
                               .voteGranted(voteGranted)
                               .term(zomkyStorage.getTerm());
                   });
    }

    private Mono<Void> startElection(Node node, ZomkyStorage zomkyStorage, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(Node::voteForMyself)
                   .then(sendVotes(node, zomkyStorage, electionContext));
    }

    private Mono<Void> sendVotes(Node node, ZomkyStorage zomkyStorage, ElectionContext electionContext) {
        return node.availableSenders()
                .flatMap(sender -> sendVoteRequest(node, zomkyStorage, sender))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > zomkyStorage.getTerm()) {
                        node.convertToFollower(voteResponse.getTerm());
                        electionContext.setRepeatElection(false);
                    }
                })
                .filter(VoteResponse::isVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(QUORUM - 1)
                .timeout(ElectionTimeout.nextRandom())
                .next()
                .doOnSuccess(s -> {
                    node.convertToLeader();
                    electionContext.setRepeatElection(false);
                })
                .onErrorResume(throwable -> {
                    boolean repeatElection = !(throwable instanceof RaftException || subscription.isDisposed());
                    if (repeatElection) {
                        LOGGER.info("[Node {}] Election timeout ({})", node.nodeId, subscription.isDisposed());
                        electionContext.setRepeatElection(repeatElection);
                    }
                    return Mono.empty();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(Node node, ZomkyStorage zomkyStorage, Sender sender) {
        LogEntryInfo last = zomkyStorage.getLast();
        VoteRequest requestVote = new VoteRequest()
                .term(zomkyStorage.getTerm())
                .candidateId(node.nodeId)
                .lastLogIndex(last.getIndex())
                .lastLogTerm(last.getTerm());

        Payload payload = ObjectPayload.create(requestVote);
        if (node.nodeState != NodeState.CANDIDATE) {
            LOGGER.info("[Node {} -> Node {}] Vote dropped", node.nodeId, sender.getNodeId());
            return Mono.just(VoteResponse.FALLBACK_RESPONSE);
        }
        return sender.getRSocket().requestResponse(payload)
                .map(payload1 -> ObjectPayload.dataFromPayload(payload1, VoteResponse.class))
                .onErrorResume(throwable -> {
                    LOGGER.error("[Node {} -> Node {}] Vote failure", node.nodeId, requestVote.getCandidateId(), throwable);
                    return Mono.just(VoteResponse.FALLBACK_RESPONSE);
                });
    }

    private Repeat<ElectionContext> leaderNotElected(ElectionContext electionContext) {
        return Repeat.<ElectionContext>onlyIf(repeatContext -> repeatContext.applicationContext().repeatElection())
                .withApplicationContext(electionContext);
    }

    private static class ElectionContext {

        private volatile boolean repeatElection;

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
