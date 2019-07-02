package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.VoteRequest;
import io.github.pmackowski.rsocket.raft.rpc.VoteResponse;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

public class CandidateRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRole.class);

    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.CANDIDATE;
    }

    @Override
    public void onInit(DefaultRaftServer node, RaftStorage raftStorage) {
        if (node.quorum() == 1)  {
            node.voteForMyself();
            node.convertToLeader();
        } else {
            ElectionContext electionContext = new ElectionContext(false);
            subscription = Mono.defer(() -> startElection(node, raftStorage, electionContext))
                    .repeatWhen(leaderNotElected(electionContext))
                    .subscribe();
        }
    }

    @Override
    public void onExit(DefaultRaftServer node, RaftStorage raftStorage) {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    private Mono<Void> startElection(DefaultRaftServer node, RaftStorage raftStorage, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(DefaultRaftServer::voteForMyself)
                   .then(sendVotes(node, raftStorage, electionContext));
    }

    private Mono<Void> sendVotes(DefaultRaftServer node, RaftStorage raftStorage, ElectionContext electionContext) {
        return node.availableSenders()
                .flatMap(sender -> sendVoteRequest(node, raftStorage, sender))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > raftStorage.getTerm()) {
                        node.convertToFollower(voteResponse.getTerm());
                        electionContext.setRepeatElection(false);
                    }
                })
                .filter(VoteResponse::getVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(node.quorum() - 1)
                .timeout(node.nextElectionTimeout())
                .next()
                .doOnSuccess(s -> {
                    node.convertToLeader();
                    electionContext.setRepeatElection(false);
                })
                .onErrorResume(throwable -> {
                    // TODO what if subscription is null ??
                    boolean repeatElection = !(throwable instanceof RaftException || subscription.isDisposed());
                    if (repeatElection) {
                        LOGGER.info("[RaftServer {}] Election timeout ({})", node.nodeId, subscription.isDisposed());
                        electionContext.setRepeatElection(repeatElection);
                    }
                    return Mono.empty();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(DefaultRaftServer node, RaftStorage raftStorage, Sender sender) {
        if (!node.isCandidate()) {
            LOGGER.info("[RaftServer {} -> RaftServer {}] Vote dropped", node.nodeId, sender.getNodeId());
            return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
        }

        IndexedTerm last = raftStorage.getLastIndexedTerm();
        VoteRequest requestVote = VoteRequest.newBuilder()
                .setTerm(raftStorage.getTerm())
                .setCandidateId(node.nodeId)
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();
        return sender.requestVote(requestVote)
                .onErrorResume(throwable -> {
                    LOGGER.error("[RaftServer {} -> RaftServer {}] Vote failure", node.nodeId, requestVote.getCandidateId(), throwable);
                    return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
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
