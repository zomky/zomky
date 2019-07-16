package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedTerm;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerRole implements RaftRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerRole.class);

    private static DirectProcessor<Long> processor;
    private static FluxSink<Long> sink;

    static {
        // optimization
        processor = DirectProcessor.create();
        sink = processor.sink(FluxSink.OverflowStrategy.DROP);
    }

    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.FOLLOWER;
    }

    @Override
    public void onInit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (raftGroup.quorum() == 1) {
            raftGroup.convertToCandidate();
        } else {
            subscription = processor.timeout(raftGroup.nextElectionTimeout())
                    .onErrorResume(throwable -> {
                        LOGGER.info("[Node {}] Election timeout ({})", node.getNodeId(), throwable.getMessage());
                        if (raftGroup.isPreVote() && raftGroup.quorum() - 1 > 0) {
                            return sendPreVotes(node, raftGroup, raftStorage)
                                    .doOnNext(preVotes -> {
                                        if (preVotes) {
                                            raftGroup.convertToCandidate();
                                        } else {
                                            raftGroup.refreshFollower();
                                        }
                                    })
                                    .then(Mono.empty());
                        } else {
                            raftGroup.convertToCandidate();
                            return Mono.empty();
                        }
                    })
                    .subscribe();
        }
    }

    @Override
    public void onExit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public Mono<VoteResponse> onRequestVote(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, VoteRequest requestVote) {
        Mono<VoteResponse> voteResponse = RaftRole.super.onRequestVote(node, raftGroup, raftStorage, requestVote);
        return voteResponse.doOnNext(r -> {
            if (r.getVoteGranted()) {
                restartElectionTimer(node);
            }
        });
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .doOnNext(appendEntriesRequest -> {
                       int currentTerm = raftStorage.getTerm();
                       if (appendEntriesRequest.getTerm() >= currentTerm) {
                           restartElectionTimer(node);
                       }
                   }).then(RaftRole.super.onAppendEntries(node, raftGroup, raftStorage, appendEntries));
    }

    private void restartElectionTimer(InnerNode node) {
        try {
            if (subscription != null && !subscription.isDisposed()) {
                LOGGER.debug("[Node {}] restartElectionTimer ...", node.getNodeId());
                sink.next(System.currentTimeMillis());
            }
        } catch (Exception e) {
            LOGGER.error("[Node {}] restartElectionTimer ... {}", node.getNodeId(), e);
        }
    }

    private Mono<Boolean> sendPreVotes(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        Duration timeout = raftGroup.nextElectionTimeout();
        return raftGroup.availableSenders()
                    .flatMap(sender -> sendPreVoteRequest(node, raftGroup, raftStorage, sender, timeout))
                    .filter(PreVoteResponse::getVoteGranted)
                    .buffer(raftGroup.quorum() - 1)
                    .timeout(timeout)
                    .next()
                    .map(i -> true)
                    .doOnError(throwable -> LOGGER.warn(String.format("[Node %s] sendPreVote failed!", node.getNodeId()), throwable))
                    .onErrorReturn(false);
    }

    private Mono<PreVoteResponse> sendPreVoteRequest(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, Sender sender, Duration timeout) {
        IndexedTerm last = raftStorage.getLastIndexedTerm();
        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setNextTerm(raftStorage.getTerm() + 1)
                .setCandidateId(node.getNodeId())
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        return sender.requestPreVote(raftGroup, preVoteRequest)
                     .timeout(timeout)
                     .onErrorResume(throwable -> {
                        LOGGER.error("[Node {} -> Node {}] Pre-Vote failure", node.getNodeId(), preVoteRequest.getCandidateId(), throwable);
                        return Mono.just(PreVoteResponse.newBuilder().setVoteGranted(false).build());
                     });
    }

}
