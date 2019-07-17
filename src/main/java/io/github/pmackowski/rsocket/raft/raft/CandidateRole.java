package io.github.pmackowski.rsocket.raft.raft;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedTerm;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.VoteRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

public class CandidateRole implements RaftRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRole.class);

    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.CANDIDATE;
    }

    @Override
    public void onInit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (raftGroup.quorum() == 1)  {
            raftGroup.voteForMyself();
            raftGroup.convertToLeader();
        } else {
            ElectionContext electionContext = new ElectionContext(false);
            subscription = Mono.defer(() -> startElection(node, raftGroup, raftStorage, electionContext))
                    .repeatWhen(leaderNotElected(electionContext))
                    .subscribe();
        }
    }

    @Override
    public void onExit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    private Mono<Void> startElection(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, ElectionContext electionContext) {
        return Mono.just(raftGroup)
                   .doOnNext(RaftGroup::voteForMyself)
                   .then(sendVotes(node, raftGroup, raftStorage, electionContext));
    }


    private Mono<Void> sendVotes(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, ElectionContext electionContext) {
        return raftGroup.availableSenders()
                .flatMap(sender -> sendVoteRequest(node, raftGroup, raftStorage, sender))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > raftStorage.getTerm()) {
                        raftGroup.convertToFollower(voteResponse.getTerm());
                        electionContext.setRepeatElection(false);
                    }
                })
                .filter(VoteResponse::getVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(raftGroup.quorum() - 1)
                .timeout(raftGroup.nextElectionTimeout())
                .next()
                .doOnSuccess(s -> {
                    raftGroup.convertToLeader();
                    electionContext.setRepeatElection(false);
                })
                .onErrorResume(throwable -> {
                    // TODO what if subscription is null ??
                    boolean repeatElection = !(throwable instanceof RaftException || subscription.isDisposed());
                    if (repeatElection) {
                        LOGGER.info("[Node {}] Election timeout ({})", node.getNodeId(), subscription.isDisposed());
                        electionContext.setRepeatElection(repeatElection);
                    }
                    return Mono.empty();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, Sender sender) {
        if (!raftGroup.isCandidate()) {
            LOGGER.info("[Node {} -> Node {}] Vote dropped", node.getNodeId(), sender.getNodeId());
            return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
        }

        IndexedTerm last = raftStorage.getLastIndexedTerm();
        VoteRequest requestVote = VoteRequest.newBuilder()
                .setTerm(raftStorage.getTerm())
                .setCandidateId(node.getNodeId())
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();
        return sender.requestVote(raftGroup, requestVote)
                .onErrorResume(throwable -> {
                    LOGGER.error("[Node {} -> Node {}] Vote failure", node.getNodeId(), requestVote.getCandidateId(), throwable);
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
